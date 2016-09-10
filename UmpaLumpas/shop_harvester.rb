#
# Tiendas y Comercios Centenarios en Madrid.
#
# Fuente: http://www.esmadrid.com/opendata/tiendas_v1_es.xml
#

require 'json'
require 'uri'
require 'net/http'
require 'rexml/document'

require_relative './harvester'

class ShopHarvester < Harvester
	#
	attr :datagrams

	#
	#
	#
	def initialize
		@collection_name = "TURISMO.tiendas"
		@datagrams = Array.new

		document = download_xml()

		process_xml_document(document)

		@bunny = BunnyClient.new

		@datagrams.each do | datagram |
			rabbit_message = {
				"target" => "datagramServer", 
				"vep_data" => [ datagram ]
			}

			publish_message(rabbit_message, "desappstre.shops") 
		end

		puts(@bunny.status_message())
	end

	private

	#
	# Descargamos el archivo Xml
	#
	def download_xml
		uri = URI.parse("http://www.esmadrid.com/opendata/tiendas_v1_es.xml")
		response = Net::HTTP.get_response(uri)
		content = response.body

		xml = REXML::Document.new(content)

		return xml
	end

	#
	# Procesamos los archivos Xml
	#
	def process_xml_document(document)
		document.elements.each('serviceList/service') do | service |
			shop_hash = Hash.new

			shop_hash["shop_id"] = service.attributes["id"]
			shop_hash["shop_updated_at"] = service.attributes["fechaActualizacion"]

			service.elements.each('basicData') do | basic |
				(name, email, phone, fax, title, body, web) = process_basic(basic)

				shop_hash["shop_name"] = name
				shop_hash["shop_email"] = email
				shop_hash["shop_phone"] = phone
				shop_hash["shop_fax"] = fax
				shop_hash["shop_title"] = title
				shop_hash["shop_body"] = body
				shop_hash["shop_web"] = web
			end

			service.elements.each('geoData') do | geo |
				(calle, zipcode, locality, country, latitude, longitude, subAdministrativeArea) = process_geo(geo)

				shop_hash["shop_street"] = calle
				shop_hash["shop_zipcode"] = zipcode
				shop_hash["shop_locality"] = locality
				shop_hash["shop_country"] = country
				shop_hash["shop_coordinates_latitude"] = latitude
				shop_hash["shop_coordinates_longitude"] = longitude
				shop_hash["shop_subarea"] = subAdministrativeArea
			end

			service.elements.each('extradata') do | extra |
				(id_tipo, tipo, pago, horario, id_categoria, categoria) = process_extra(extra)

				shop_hash["shop_type_id"] = id_tipo
				shop_hash["shop_type"] = tipo
				shop_hash["shop_category_id"] = id_categoria
				shop_hash["shop_category"] = categoria

				if pago != "" then
					shop_hash["shop_payments"] = pago
				end

				if horario != "" then
					shop_hash["shop_timetable"] = horario
				end 
			end
			
			datagram = compose_datagram(shop_hash)
			@datagrams.push(datagram)

			shop_hash.clear()
		end
	end

	#
	# Genera un datagrama para cada comercio
	#
	def compose_datagram(shop_hash)
		coordenadas = { 
			"lat" => shop_hash["shop_coordinates_latitude"].to_f, 
			"lon" => shop_hash["shop_coordinates_longitude"].to_f 
		}

		fecha = Date.strptime(shop_hash["shop_updated_at"], "%Y-%m-%d")
		instant = fecha.strftime("%Y-%m-%d %H:%M:%S.001") 
		
		datagram = create_base_hash(shop_hash["shop_id"], coordenadas, shop_hash["shop_name"], instant, "fa-shopping-bag", "green")
		
		data_hash = Hash.new

		shop_hash.each do | key, value |
			if value != nil then
				data_hash[key] = value
			end
		end

		datagram["layerData"]["shopData"] = data_hash
		
		return datagram
	end
end
