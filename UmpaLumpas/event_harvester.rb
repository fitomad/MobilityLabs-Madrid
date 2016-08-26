#
# Agenda de Eventos en Madrid.
#
# Fuente: http://www.esmadrid.com/opendata/agenda_v1_es.xml
#

require 'date'
require 'json'
require 'uri'
require 'net/http'
require 'rexml/document'

require_relative './harvester'

class EventHarvester < Harvester
	#
	#
	#
	def initialize
		@collection_name = "TURISMO.eventos"
		@datagrams = Array.new

		document = download_xml()

		process_xml_document(document)

		@bunny = BunnyClient.new

		@datagrams.each do | datagram |
			rabbit_message = {
				"target" => "datagramServer", 
				"vep_data" => [ datagram ]
			}

			publish_message(rabbit_message, "desappstre.events") 
		end

		@bunny.close()

		puts(@bunny.status_message())
	end

	private

	#
	# Descargamos el archivo Xml con la información de eventos
	# para el día en curso.
	#
	def download_xml
		uri = URI.parse("http://www.esmadrid.com/opendata/agenda_v1_es.xml")
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
			event_hash = Hash.new

			event_hash["event_id"] = service.attributes["id"]
			event_hash["event_updated_at"] = service.attributes["fechaActualizacion"]

			service.elements.each('basicData') do | basic |
				(name, email, phone, fax, title, body, web, venue_id, venue_name) = process_basic(basic)

				event_hash["event_name"] = name
				event_hash["event_email"] = email
				event_hash["event_phone"] = phone
				event_hash["event_fax"] = fax
				event_hash["event_title"] = title
				event_hash["event_body"] = body
				event_hash["event_web"] = web

				venue_hash = {
					"venue_id" => venue_id,
					"venue_name" => venue_name
				}

				event_hash["venue"] = venue_hash
			end

			service.elements.each('geoData') do | geo |
				(calle, zipcode, locality, country, latitude, longitude, subAdministrativeArea) = process_geo(geo)

				event_hash["event_street"] = calle
				event_hash["event_zipcode"] = zipcode
				event_hash["event_locality"] = locality
				event_hash["event_country"] = country
				event_hash["event_coordinates_latitude"] = latitude
				event_hash["event_coordinates_longitude"] = longitude
				event_hash["event_subarea"] = subAdministrativeArea
			end

			service.elements.each('extradata') do | extra |
				(id_tipo, tipo, pago, horario, id_categoria, categoria, id_subcategoria, subcategoria, start_date, end_date, day_week, exclusions, inclusions) = process_extra(extra)

				event_hash["event_type_id"] = id_tipo
				event_hash["event_type"] = tipo
				event_hash["event_category_id"] = id_categoria
				event_hash["event_category"] = categoria
				event_hash["event_subcategory_id"] = id_subcategoria
				event_hash["event_subcategory"] = subcategoria

				if pago != nil then
					event_hash["event_payments"] = pago
				end

				if horario != nil then
					event_hash["event_timetable"] = horario
				end

				schedule_hash = Hash.new

				schedule_hash["start_date"] = start_date

				if end_date != nil
					schedule_hash["end_date"] = end_date 
				end

				if day_week != nil
					days = day_week.split(",")
					days.map! do | item | 
						item.to_i
					end

					schedule_hash["days"] = days
				end

				if exclusions != nil
					schedule_hash["exclusions"] = exclusions
				end

				if inclusions != nil
					schedule_hash["inclusions"] = inclusions
				end

				event_hash["schedule"] = schedule_hash
			end
			
			datagram = compose_datagram(event_hash)
			@datagrams.push(datagram)

			event_hash.clear()
		end
	end

	#
	# Los eventos requieren de un tratamiento especial en lo que 
	# a la sección extra se refiere ya que incorporan datos
	# acerca del horario y fechas en los que tendrá lugar.
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
		start_date = ""
		end_date = ""
		day_week = ""
		inclusions = nil
		exclusions = nil

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

		if node.elements["fechas/rango"] != nil
			node.elements.each('fechas/rango') do | fechas |
				start_date = fechas.elements["inicio"].text
				end_date = fechas.elements["fin"].text
				day_week = fechas.elements["dias"].text
			end
		else
			start_date = node.elements["fechas/inclusion"].text
		end

		if node.elements["fechas/exclusion"] != nil
			exclusions = Array.new

			node.elements.each("fechas/exclusion") do | exclusion |
				exclusions.push(exclusion.text)
			end
		end

		if node.elements["fechas/inclusion"] != nil
			inclusions = Array.new

			node.elements.each("fechas/inclusion") do | inclusion |
				inclusions.push(inclusion.text)
			end
		end

		return id_tipo, tipo, pago, horario, id_categoria, category, id_subcategoria, subcategory, start_date, end_date, day_week, exclusions, inclusions
	end

	#
	# Construye el datagrama con los datos del evento 
	#
	def compose_datagram(event_hash)
		coordenadas = { 
			"lat" => event_hash["event_coordinates_latitude"].to_f, 
			"lon" => event_hash["event_coordinates_longitude"].to_f 
		}

		fecha = Date.strptime(event_hash["schedule"]["start_date"], "%d/%m/%Y")
		instant = fecha.strftime("%Y-%m-%d %H:%M:%S.001")
		
		datagram = create_base_hash(event_hash["event_id"], coordenadas, event_hash["event_name"], instant, "fa-map-marker", "blue")
		
		data_hash = Hash.new

		event_hash.each do | key, value |
			if value != nil then
				data_hash[key] = value
			end
		end

		datagram["layerData"]["eventData"] = data_hash
		
		return datagram
	end
end
