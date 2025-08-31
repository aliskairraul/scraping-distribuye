provincias_general = ['Álava', 'Albacete', 'Alicante', 'Almería', 'Ávila', 'Badajoz', 'Islas Baleares', 'Barcelona', 'Burgos', 'Cáceres', 'Cádiz',
                      'Castellón', 'Ciudad Real', 'Córdoba', 'A Coruña', 'Cuenca', 'Girona', 'Granada', 'Guadalajara', 'Guipúzcoa', 'Huelva',
                      'Huesca', 'Jaén', 'León', 'Lleida', 'La Rioja', 'Lugo', 'Madrid', 'Málaga', 'Murcia', 'Navarra', 'Ourense', 'Asturias',
                      'Palencia', 'Las Palmas', 'Pontevedra', 'Salamanca', 'Santa Cruz de Tenerife', 'Cantabria', 'Segovia', 'Sevilla', 'Soria',
                      'Tarragona', 'Teruel', 'Toledo', 'Valencia', 'Valladolid', 'Vizcaya', 'Zamora', 'Zaragoza', 'Ceuta', 'Melilla']

provincias_urls_infoempleo = ['alava', 'albacete', 'alicante', 'almeria', 'avila', 'badajoz', 'islas-baleares', 'barcelona', 'burgos', 'caceres', 'cadiz',
                              'castellon', 'ciudad-real', 'cordoba', 'a-corunna', 'cuenca', 'girona', 'granada', 'guadalajara', 'guipuzcoa', 'huelva',
                              'huesca', 'jaen', 'leon', 'lleida', 'la-rioja', 'lugo', 'madrid', 'malaga', 'murcia', 'navarra', 'ourense', 'asturias',
                              'palencia', 'las-palmas', 'pontevedra', 'salamanca', 'tenerife', 'cantabria', 'segovia', 'sevilla', 'soria',
                              'tarragona', 'teruel', 'toledo', 'valencia', 'valladolid', 'vizcaya', 'zamora', 'zaragoza', 'ceuta', 'melilla']

provincia_infoempleo = provincias_urls_infoempleo[0]
url_provincia = f'https://www.infoempleo.com/trabajo/en_{provincia_infoempleo}/?diasPublicacion=1&ordenacion=fechaAlta'
