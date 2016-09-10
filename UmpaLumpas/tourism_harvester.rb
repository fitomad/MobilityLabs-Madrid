#
# Lugares de interés turístico en Madrid.
#
# Fuente: http://www.esmadrid.com/opendata/turismo_v1_es.xml
#

require 'json'
require 'uri'
require 'net/http'
require 'rexml/document'

require_relative './harvester'

class TourismHarvester < Harvester
	#
	attr :datagrams

	#
	#
	#
	def initialize
		@collection_name = "TURISMO.lugares"
		@datagrams = Array.new

		document = download_xml()

		process_xml_document(document)

		@bunny = BunnyClient.new

		@datagrams.each do | datagram |
			rabbit_message = {
				"target" => "datagramServer", 
				"vep_data" => [ datagram ]
			}

			publish_message(rabbit_message, "desappstre.venues") 
		end

		puts(@bunny.status_message())
	end

	private

	#
	# Descargamos el archivo Xml
	#
	def download_xml
		uri = URI.parse("http://www.esmadrid.com/opendata/turismo_v1_es.xml")
		response = Net::HTTP.get_response(uri)
		content = response.body

		xml = REXML::Document.new(content)

		return xml
	end

	#
	# Procesamos el archivo
	#
	def process_xml_document(document)
		document.elements.each('serviceList/service') do | service |
			venue_hash = Hash.new

			venue_hash["venue_id"] = service.attributes["id"]
			venue_hash["venue_updated_at"] = service.attributes["fechaActualizacion"]

			service.elements.each('basicData') do | basic |
				(name, email, phone, fax, title, body, web) = process_basic(basic)

				venue_hash["venue_name"] = name
				venue_hash["venue_email"] = email
				venue_hash["venue_phone"] = phone
				venue_hash["venue_fax"] = fax
				venue_hash["venue_title"] = title
				venue_hash["venue_body"] = body
				venue_hash["venue_web"] = web
			end

			service.elements.each('geoData') do | geo |
				(calle, zipcode, locality, country, latitude, longitude, subAdministrativeArea) = process_geo(geo)

				venue_hash["venue_street"] = calle
				venue_hash["venue_zipcode"] = zipcode
				venue_hash["venue_locality"] = locality
				venue_hash["venue_country"] = country
				venue_hash["venue_coordinates_latitude"] = latitude
				venue_hash["venue_coordinates_longitude"] = longitude
				venue_hash["venue_subarea"] = subAdministrativeArea
			end

			service.elements.each('extradata') do | extra |
				(id_tipo, tipo, pago, horario, id_categoria, categoria) = process_extra(extra)

				venue_hash["venue_type_id"] = id_tipo
				venue_hash["venue_type"] = tipo
				venue_hash["venue_category_id"] = id_categoria
				venue_hash["venue_category"] = categoria

				if pago != "" then
					venue_hash["venue_payments"] = pago
				end

				if horario != "" then
					venue_hash["venue_timetable"] = horario
				end 
			end
			
			datagram = compose_datagram(venue_hash)
			@datagrams.push(datagram)

			venue_hash.clear()
		end
	end

	#
	# Generamos el datagrama para cada uno de los lugares
	#
	def compose_datagram(venue_hash)
		coordenadas = { 
			"lat" => venue_hash["venue_coordinates_latitude"].to_f, 
			"lon" => venue_hash["venue_coordinates_longitude"].to_f 
		}

		fecha = Date.strptime(venue_hash["venue_updated_at"], "%Y-%m-%d")
		instant = fecha.strftime("%Y-%m-%d %H:%M:%S.001") 
		
		datagram = create_base_hash(venue_hash["venue_id"], coordenadas, venue_hash["venue_name"], instant, "fa-map-marker", "blue")
		
		data_hash = Hash.new

		venue_hash.each do | key, value |
			if value != nil then
				data_hash[key] = value
			end
		end

		datagram["layerData"]["venueData"] = data_hash
		
		return datagram
	end
end
