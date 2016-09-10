#
# Restaurantes en Madrid.
#
# Fuente: http://www.esmadrid.com/opendata/restaurantes_v1_es.xml
#

require 'date'
require 'json'
require 'uri'
require 'net/http'
require 'rexml/document'

require_relative './harvester'

class RestaurantHarvester < Harvester
	#
	#
	#
	def initialize
		@collection_name = "TURISMO.restaurantes"
		@datagrams = Array.new

		document = download_xml()

		process_xml_document(document)

		@bunny = BunnyClient.new

		@datagrams.each do | datagram |
			rabbit_message = {
				"target" => "datagramServer", 
				"vep_data" => [ datagram ]
			}

			publish_message(rabbit_message, "desappstre.restaurant") 
		end

		puts(@bunny.status_message())
	end

	private

	#
	# Descargamos el archivo Xml
	#
	def download_xml
		uri = URI.parse("http://www.esmadrid.com/opendata/restaurantes_v1_es.xml")
		response = Net::HTTP.get_response(uri)
		content = response.body

		xml = REXML::Document.new(content)

		return xml
	end

	#
	# Procesamos el archivo Xml
	#
	def process_xml_document(document)
		document.elements.each('serviceList/service') do | service |
			restaurant_hash = Hash.new

			restaurant_hash["restaurant_id"] = service.attributes["id"]
			restaurant_hash["restaurant_updated_at"] = service.attributes["fechaActualizacion"]

			service.elements.each('basicData') do | basic |
				(name, email, phone, fax, title, body, web, venue_id, venue_name) = process_basic(basic)

				restaurant_hash["restaurant_name"] = name
				restaurant_hash["restaurant_email"] = email
				restaurant_hash["restaurant_phone"] = phone
				restaurant_hash["restaurant_fax"] = fax
				restaurant_hash["restaurant_title"] = title
				restaurant_hash["restaurant_body"] = body
				restaurant_hash["restaurant_web"] = web
			end

			service.elements.each('geoData') do | geo |
				(calle, zipcode, locality, country, latitude, longitude, subAdministrativeArea) = process_geo(geo)

				restaurant_hash["restaurant_street"] = calle
				restaurant_hash["restaurant_zipcode"] = zipcode
				restaurant_hash["restaurant_locality"] = locality
				restaurant_hash["restaurant_country"] = country
				restaurant_hash["restaurant_coordinates_latitude"] = latitude
				restaurant_hash["restaurant_coordinates_longitude"] = longitude
				restaurant_hash["restaurant_subarea"] = subAdministrativeArea
			end

			service.elements.each('extradata') do | extra |
				(id_tipo, tipo, pago, horario, id_categoria, categoria, id_subcategoria, subcategoria) = process_extra(extra)

				restaurant_hash["restaurant_type_id"] = id_tipo
				restaurant_hash["restaurant_type"] = tipo
				restaurant_hash["restaurant_category_id"] = id_categoria
				restaurant_hash["restaurant_category"] = categoria
				restaurant_hash["restaurant_subcategory_id"] = id_subcategoria
				restaurant_hash["restaurant_subcategory"] = subcategoria

				if pago != nil then
					restaurant_hash["restaurant_payments"] = pago
				end

				if horario != nil then
					restaurant_hash["restaurant_timetable"] = horario
				end
			end
			
			datagram = compose_datagram(restaurant_hash)
			@datagrams.push(datagram)

			restaurant_hash.clear()
		end
	end

	#
	# Sobreescribimos el método porque los restaurantes
	# incluyen información de Subcategorías
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
	# Datagrama para cada restaurante
	#
	def compose_datagram(restaurant_hash)
		coordenadas = { 
			"lat" => restaurant_hash["restaurant_coordinates_latitude"].to_f, 
			"lon" => restaurant_hash["restaurant_coordinates_longitude"].to_f 
		}

		fecha = Date.strptime(restaurant_hash["restaurant_updated_at"], "%Y-%m-%d")
		instant = fecha.strftime("%Y-%m-%d %H:%M:%S.001")
		
		datagram = create_base_hash(restaurant_hash["restaurant_id"], coordenadas, restaurant_hash["restaurant_name"], instant, "fa-cutlery", "black")
		
		data_hash = Hash.new

		restaurant_hash.each do | key, value |
			if value != nil then
				data_hash[key] = value
			end
		end

		datagram["layerData"]["restaurantData"] = data_hash
		
		return datagram
	end
end
