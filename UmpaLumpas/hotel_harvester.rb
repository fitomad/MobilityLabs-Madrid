#
# Alojamientos en Madrid.
#
# Fuente: http://www.esmadrid.com/opendata/alojamientos_v1_es.xml
#

require 'date'
require 'json'
require 'uri'
require 'net/http'
require 'rexml/document'

require_relative './harvester'

class HotelHarvester < Harvester
	#
	#
	#
	def initialize
		@collection_name = "TURISMO.hoteles"
		@datagrams = Array.new

		document = download_xml()

		process_xml_document(document)

		@bunny = BunnyClient.new

		@datagrams.each do | datagram |
			rabbit_message = {
				"target" => "datagramServer", 
				"vep_data" => [ datagram ]
			}

			publish_message(rabbit_message, "desappstre.hotels") 
		end

		@bunny.close()

		puts(@bunny.status_message())
	end

	private

	#
	# Descargamos el archivo de alojamientos.
	#
	def download_xml
		uri = URI.parse("http://www.esmadrid.com/opendata/alojamientos_v1_es.xml")
		response = Net::HTTP.get_response(uri)
		content = response.body

		xml = REXML::Document.new(content)

		return xml
	end

	#
	# Procesa el documento Xml
	#
	def process_xml_document(document)
		document.elements.each('serviceList/service') do | service |
			hotel_hash = Hash.new

			hotel_hash["hotel_id"] = service.attributes["id"]
			hotel_hash["hotel_updated_at"] = service.attributes["fechaActualizacion"]

			service.elements.each('basicData') do | basic |
				(name, email, phone, fax, title, body, web, hotel_id, hotel_name) = process_basic(basic)

				hotel_hash["hotel_name"] = name
				hotel_hash["hotel_email"] = email
				hotel_hash["hotel_phone"] = phone
				hotel_hash["hotel_fax"] = fax
				hotel_hash["hotel_title"] = title
				hotel_hash["hotel_body"] = body
				hotel_hash["hotel_web"] = web
			end

			service.elements.each('geoData') do | geo |
				(calle, zipcode, locality, country, latitude, longitude, subAdministrativeArea) = process_geo(geo)

				hotel_hash["hotel_street"] = calle
				hotel_hash["hotel_zipcode"] = zipcode
				hotel_hash["hotel_locality"] = locality
				hotel_hash["hotel_country"] = country
				hotel_hash["hotel_coordinates_latitude"] = latitude
				hotel_hash["hotel_coordinates_longitude"] = longitude
				hotel_hash["hotel_subarea"] = subAdministrativeArea
			end

			service.elements.each('extradata') do | extra |
				(id_tipo, tipo, pago, horario, id_categoria, categoria, id_subcategoria, subcategoria) = process_extra(extra)

				hotel_hash["hotel_type_id"] = id_tipo
				hotel_hash["hotel_type"] = tipo
				hotel_hash["hotel_category_id"] = id_categoria
				hotel_hash["hotel_category"] = categoria
				hotel_hash["hotel_subcategory_id"] = id_categoria
				hotel_hash["hotel_subcategory"] = categoria	

				if pago != "" then
					hotel_hash["hotel_payments"] = pago
				end

				if horario != "" then
					hotel_hash["hotel_timetable"] = horario
				end			
			end
			
			datagram = compose_datagram(hotel_hash)
			@datagrams.push(datagram)

			hotel_hash.clear()
		end
	end

	#
	# Los hoteles contienen información 
	# de Subcatecorías
	#
	def process_extra(node)
		id_tipo = ""
		tipo = ""
		pago = ""
		horario = ""
		id_categoria = ""
		category = ""
		id_subcategoria = ""
		subcategory = ""

		node.elements.each('item') do | item |
			name = item.attributes["name"]

			case name
				when "idTipo"
					id_tipo = item.text
				when "Tipo"
					tipo = item.text
				when "Servicios de pago"
					pago = item.text
				when "Horario"
					horario = item.text
			end
		end

		node.elements.each('categorias/categoria/item') do | categoria |
			name = categoria.attributes["name"]

			case name
				when "idCategoria"
					id_categoria = categoria.text
				when "Categoria"
					category = categoria.text
			end
		end

		node.elements.each('categorias/categoria/subcategorias/subcategoria/item') do | subcategoria |
			name = subcategoria.attributes["name"]

			case name
				when "idSubCategoria"
					id_subcategoria = subcategoria.text
				when "SubCategoria"
					subcategory = subcategoria.text
			end
		end
		
		return id_tipo, tipo, pago, horario, id_categoria, category, id_subcategoria, subcategory
	end

	#
	# Construye un datagrama para un alojamiento en concreto
	#
	def compose_datagram(hotel_hash)
		coordenadas = { 
			"lat" => hotel_hash["hotel_coordinates_latitude"].to_f, 
			"lon" => hotel_hash["hotel_coordinates_longitude"].to_f 
		}

		fecha = Date.strptime(hotel_hash["hotel_updated_at"], "%Y-%m-%d")
		instant = fecha.strftime("%Y-%m-%d %H:%M:%S.001")
		
		datagram = create_base_hash(hotel_hash["hotel_id"], coordenadas, hotel_hash["hotel_name"], instant, "fa-bed", "purple")
		
		data_hash = Hash.new

		hotel_hash.each do | key, value |
			if value != nil then
				data_hash[key] = value
			end
		end

		datagram["layerData"]["hotelData"] = data_hash
		
		return datagram
	end
end
