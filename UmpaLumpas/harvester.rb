require 'date'
require 'json'
require 'uri'
require 'net/http'
require 'rexml/document'

require_relative './utils/bunny_client.rb'

#
# Clase base con método de ayuda para el 
# proceso ETL de los datos de esmadrid
#
class Harvester
	#
	attr_accessor :datagrams
	# El cliente RabbitMQ
	attr_accessor :bunny
	# Nombre de la coleccion sobre la que vamos
	# a operarar
	attr_writer :collection_name

	#
	# Extrae la información del nodo serviceList/service/basicData
	#
	def process_basic(node)
		name = node.elements["name"].text
		email = node.elements["email"].text
		phone = node.elements["phone"].text
		fax = node.elements["fax"].text
		title = node.elements["title"].text
		body = node.elements["body"].text
		web = node.elements["web"].text

		venue_id = nil
		venue_name = nil

		if node.elements["idrt"] != nil then
			venue_id = node.elements["idrt"].text
		end

		if node.elements["nombrert"] != nil then
			venue_name = node.elements["nombrert"].text
		end

		return name, email, phone, fax, title, body, web, venue_id, venue_name
	end

	#
	# Extrae la información del nodo serviceList/service/geoData
	#
	def process_geo(node)
		calle = node.elements["address"].text
		zipcode = node.elements["zipcode"].text
		locality = node.elements["locality"].text
		country = node.elements["country"].text
		latitude = node.elements["latitude"].text
		longitude = node.elements["longitude"].text
		subAdministrativeArea = node.elements["subAdministrativeArea"].text

		return calle, zipcode, locality, country, latitude, longitude, subAdministrativeArea
	end

	#
	# Extrae la información del nodo serviceList/service/extradata
	#
	def process_extra(node)
		id_tipo = ""
		tipo = ""
		pago = ""
		horario = ""
		id_categoria = ""
		category = ""

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

		return id_tipo, tipo, pago, horario, id_categoria, category
	end

	#
	# Compone el datagrama que se envía de RabbitMQ a falta de los datos
	# específicos de cada colección.
	#
	# De esto se encargan las clases especializadas en cada uno de los tipos
	# de fuentes de datos.
	#
	def create_base_hash(datagram_id, coordinates, descripcion, state_instant, marker, color)
		geometry_hash = {
			"type" => "Point",
			"coordinates"  => [
			    coordinates["lon"],
			    coordinates["lat"]
	    	]
		}

		state_hash = {
			"description" => descripcion,
			"format" => "text"
		}

		layer_hash = {
			"_id" => datagram_id,
			"system" => "LAYERS", 
			"subsystem" => "PUTDATA", 
			"function" => "REPLACE",
			"layer" => {  
				"owner" => "WEB.SERV.adolfo.vera@outlook.com", 
				"type" => "public",     
				"name" => @collection_name
			},
			"geometry" => geometry_hash,
			"shape" => {
				"type" => "marker",
				"options" => {
					"icon" => marker,
					"markerColor" => color,
					"prefix" => "fa",
					"shape" => "circle"
				}
			},
			"state" => state_hash,
			"instant" => Time.now.strftime("%Y-%m-%d %H:%M:%S.001")
		}

		base_hash = {
			"layerData" => layer_hash,
		}

		return base_hash
	end

	#
	# Publica el mensaje en RabbitMQ
	#
	def publish_message(hash, type_name)
		rabbit_json = JSON.generate(hash)
		#puts rabbit_json
		if @bunny.ok? then
			@bunny.publish_datagram(rabbit_json, type_name)
		end
	end
end