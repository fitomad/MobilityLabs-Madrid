#
# Ocio y Vida Nocturna en Madrid.
#
# Fuente: http://www.esmadrid.com/opendata/noche_v1_es.xml
#

require 'json'
require 'uri'
require 'net/http'
require 'rexml/document'

require_relative './harvester'

class NightLifeHarvester < Harvester
	#
	attr :datagrams

	#
	#
	#
	def initialize
		@collection_name = "TURISMO.ocio"
		@datagrams = Array.new

		document = download_xml()

		process_xml_document(document)

		@bunny = BunnyClient.new

		@datagrams.each do | datagram |
			rabbit_message = {
				"target" => "datagramServer", 
				"vep_data" => [ datagram ]
			}

			publish_message(rabbit_message, "desappstre.nightlife") 
		end

		puts(@bunny.status_message())
	end

	private

	#
	# Descargamos el archivo Xml
	#
	def download_xml
		uri = URI.parse("http://www.esmadrid.com/opendata/noche_v1_es.xml")
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
			nightlife_hash = Hash.new

			nightlife_hash["nightlife_id"] = service.attributes["id"]
			nightlife_hash["nightlife_updated_at"] = service.attributes["fechaActualizacion"]

			service.elements.each('basicData') do | basic |
				(name, email, phone, fax, title, body, web) = process_basic(basic)

				nightlife_hash["nightlife_name"] = name
				nightlife_hash["nightlife_email"] = email
				nightlife_hash["nightlife_phone"] = phone
				nightlife_hash["nightlife_fax"] = fax
				nightlife_hash["nightlife_title"] = title
				nightlife_hash["nightlife_body"] = body
				nightlife_hash["nightlife_web"] = web
			end

			service.elements.each('geoData') do | geo |
				(calle, zipcode, locality, country, latitude, longitude, subAdministrativeArea) = process_geo(geo)

				nightlife_hash["nightlife_street"] = calle
				nightlife_hash["nightlife_zipcode"] = zipcode
				nightlife_hash["nightlife_locality"] = locality
				nightlife_hash["nightlife_country"] = country
				nightlife_hash["nightlife_coordinates_latitude"] = latitude
				nightlife_hash["nightlife_coordinates_longitude"] = longitude
				nightlife_hash["nightlife_subarea"] = subAdministrativeArea
			end

			service.elements.each('extradata') do | extra |
				(id_tipo, tipo, pago, horario, categories) = process_extra(extra)

				nightlife_hash["nightlife_type_id"] = id_tipo
				nightlife_hash["nightlife_type"] = tipo
				nightlife_hash["categories"] = categories

				if pago != "" then
					nightlife_hash["nightlife_payments"] = pago
				end

				if horario != "" then
					nightlife_hash["nightlife_timetable"] = horario
				end 
			end
			
			datagram = compose_datagram(nightlife_hash)
			@datagrams.push(datagram)

			nightlife_hash.clear()
		end
	end

	#
	# El elemento puede tener de 1..N categorias
	# que almacenamos en un array.
	#
	def process_extra(node)
		id_tipo = ""
		tipo = ""
		pago = ""
		horario = ""
		categories = Array.new

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

		node.elements.each('categorias/categoria') do | categoria |
			categoria_hash = Hash.new

			categoria.elements.each('item') do | item_categoria |
				name = item_categoria.attributes["name"]

				case name
					when "idCategoria"
						categoria_hash["category_id"] = item_categoria.text 
					when "Categoria"
						categoria_hash["category"] = item_categoria.text
				end
			end

			categories.push(categoria_hash)
		end

		return id_tipo, tipo, pago, horario, categories
	end

	#
	# Genera el datagrama de los locales
	#
	def compose_datagram(nightlife_hash)
		coordenadas = { 
			"lat" => nightlife_hash["nightlife_coordinates_latitude"].to_f, 
			"lon" => nightlife_hash["nightlife_coordinates_longitude"].to_f 
		}

		fecha = Date.strptime(nightlife_hash["nightlife_updated_at"], "%Y-%m-%d")
		instant = fecha.strftime("%Y-%m-%d %H:%M:%S.001") 
		
		datagram = create_base_hash(nightlife_hash["nightlife_id"], coordenadas, nightlife_hash["nightlife_name"], instant, "fa-shopping-bag", "black")
		
		data_hash = Hash.new

		nightlife_hash.each do | key, value |
			if value != nil then
				data_hash[key] = value
			end
		end

		datagram["layerData"]["nightlifeData"] = data_hash
		
		return datagram
	end
end
