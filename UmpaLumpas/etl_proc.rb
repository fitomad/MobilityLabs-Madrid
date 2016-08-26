require_relative './event_harvester'
require_relative './nightlife_harvester'
require_relative './hotel_harvester'
require_relative './restaurant_harvester'
require_relative './tourism_harvester'
require_relative './shop_harvester'

#
# Script para lanzar el proceso de carga de las 6 fuentes
# generadas por EsMadrid con datos referentes al turismo
# en la ciudad de Madrid.
#
# Frecuencia de actualización: DIARIA
#
# Web: http://www.esmadrid.com/
# Open Data: http://goo.gl/W2L0Jb 
#

puts("Proceso ETL // EsMadrid -> Mobility Labs.")

puts("(1/6) Lugares de Interés.")
tourims = TourismHarvester.new

puts("(2/6) Agenda Cultural.")
events = EventHarvester.new

puts("(3/6) Ocio y Vida Nocturna.")
night = NightLifeHarvester.new

puts("(4/6) Hoteles y Alojamientos.")
hotels = HotelHarvester.new

puts("(5/6) Restaurantes.")
restaurants = RestaurantHarvester.new

puts("(6/6) Tiendas y Comercios.")
shops = ShopHarvester.new