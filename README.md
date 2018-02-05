# MobilityLabs-Madrid
Proyecto para el [Hackatón](https://mobilitylabs.emtmadrid.es/portal/index.php/hackaton-2016/) sobre movilidad propuesto por el [MobilityLabs Madrid](https://mobilitylabs.emtmadrid.es/portal/).

## De qué va esto

El proyecto está dividido en tres áreas. Una orientada a la **información de tiempos de llegada** de los autobúses, otra en informar de las **incidencias en el servicio** y la última centrado en reunir la **información relacionada con el turismo**.

## ¿Y qué datos usas?

Para la información de llegada empleo los servicios [OpenData de la EMT Madrid](http://opendata.emtmadrid.es/Home). Son gratis y sólo necesitas [registrarte](http://opendata.emtmadrid.es/Formulario) para poder usarlos.

Las incidencias se obtienen directamente del [feed RSS](http://servicios.emtmadrid.es:8080/rss/emtrss.xml) que publica la EMT. Son públicos y no necesitas registrarte para usarlos.

En cuanto al turismo usamos los datos que diariamente genera el portal [EsMadrid](http://esmadrid.com), que es la web oficial de turismo del Ayuntamiento de Madrid.

## Cuéntame el proceso ETL

Son una serie de scripts escritos en Ruby que puedes encontrar dentro de la carpeta UmpaLumpas. El script que lanza y controla todo el proceso se llama `etl_proc.rb`. Este script contiene a su vez las llamadas a los scripts que realizan el ETL para cada una de las seis fuentes de datos que extraemos de EsMadrid.

Dentro de la carpeta *utils* se encuentra un sencillo cliente para [RabbitMQ](https://www.rabbitmq.com/) ya configurado para trabajar con el [servidor](https://mobilitylabs.emtmadrid.es/portal/index.php/servidor-de-colas/) del MobilityLabs.

### ¡Oye! ¿Por qué se llaman UmpaLumpas?
El término aparece en la película *Charlie y la fábrica de chocolate* y en uno de los capítulos de *The Big Bang Theory* Sheldon entra en el laboratorio de Howard saludando con un...

> ¡Hola UmpaLumpas de la ciencia!

Lo podéis ver mejor en este [vídeo de YouTube](https://www.youtube.com/watch?v=u_Q68mG5tKY)

Resumiendo, que me hizo gracia.

## OpenData de la EMT Madrid

Para acceder a los datos he desarrollado un cliente `Swift` que se encuentra publicado en [este repositorio](https://github.com/fitomad/EMTClient) de GitHub

## Feed de Incidencias de EMT Madrid

La información de incidencias en el servicio se consume con este cliente, también desarrollado en `Swift` y que se podéis encontrar en [este otro repositorio](https://github.com/fitomad/EMTFeedClient) de GitHub

## Contacto

Si tienes dudas, quieres decirme algo o simplemente saludar me encontrarás en mi [cuenta de twitter](https://twitter.com/fitomad)
