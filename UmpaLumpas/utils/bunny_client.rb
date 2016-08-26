#!/usr/bin/env ruby
# encoding: utf-8

require "bunny"

#
# Cliente para trabajar con el servidor RabbitMQ
# del Mobility Labs de la EMT Madrid
#
class BunnyClient
	# Configuración del servidor Rabbit
	attr :bunny_conn
	attr :channel
	# Nombre de la colección con la que trabajamos
	attr_reader :q_name
	# Usuario 
	attr_reader :q_user
	attr_reader :q_password
	# Traza del estado del envío
	attr :send_count
	attr :error_count

	#
	#
	#
	def initialize
		@send_count = 0
		@error_count = 0

		@q_name = "messages"
		@q_user = "TU_USUARIO"
		@q_password = "TU_PASSWORD"

		connection_hash = 
		{
			:host => "amqp.emtmadrid.es",
			:port => "5672",
			:user => @q_user,
			:password => @q_password,
			:auth_mechanism => "PLAIN",
			:automatically_recover => true
		}

		@bunny_conn = Bunny.new(connection_hash)
		@bunny_conn.start()

		@channel = @bunny_conn.create_channel()
	end

	#
	# ¿Todo listo para trabajar con Rabbit 
	# y la cola especificada
	#
	def ok?
		return @bunny_conn.queue_exists?(@q_name) 
	end

	#
	# Cerramos la conexión
	#
	def close
		@channel.close()
		@bunny_conn.close()
	end

	#
	# Muestra en consola el estado del envío.
	#
	def status_message
		return "Send: #{send_count} Error: #{error_count}"
	end

	#
	# Publicamos el mensaje en Rabbit
	#
	def publish_datagram(datagram, type)
		exchange = @channel.default_exchange()

		exchange.on_return do | return_info, properties, content |
			@error_count += 1
  			print(status_message())
  			print("\r")
		end

		exchange.publish(datagram, 
			:mandatory => true, 
			:routing_key => "messages", 
			:type => type,
			:content_type => "application/json",
			:user_id => @q_user)

		@send_count += 1

		print(status_message())
		print("\r")
	end
end
