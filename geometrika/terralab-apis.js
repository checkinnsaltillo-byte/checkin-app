/* TerraLab · muestras de estructura de APIs externas (solo lo que cada proveedor publica; revisado 2026-11-26) */
window.TLAPIS = [
 {
  "id": "bd",
  "n": "Bright Data · Dataset Inmuebles24",
  "tipo": "Dataset comprado",
  "c": "#22d3ee",
  "url": "https://brightdata.com/products/datasets/inmuebles24",
  "costo": "Desde 0.0025 USD por registro; mínimo 250 USD (≈100 mil registros), pago único. Suscripción con descuentos de 25 % a 80 %.",
  "acceso": "Compra en el Marketplace de Bright Data; se puede filtrar por geografía, periodo o cualquier campo y ocultar columnas.",
  "entrega": "JSON, NDJSON, CSV, XLSX o Parquet (con .gz). Entrega a S3, GCS, Azure, Snowflake, SFTP, webhook o correo. Actualización mensual, semanal, diaria o una vez.",
  "tam": "≈972 mil registros · 38 campos",
  "campos": [
   [
    "url",
    "string"
   ],
   [
    "Title",
    "string"
   ],
   [
    "GeneratedTitle",
    "string"
   ],
   [
    "Description",
    "string"
   ],
   [
    "Seller",
    "string"
   ],
   [
    "Precio",
    "string"
   ],
   [
    "Publicado_hace",
    "string"
   ],
   [
    "CreatedDate",
    "string"
   ]
  ],
  "nota": "La página solo confirma estos 8 de los 38 campos y menciona además recámaras y coordenadas sin dar sus nombres exactos. La lista completa está en la pestaña «Data Dictionary» y hay una muestra gratis que se descarga desde el Marketplace; no está disponible sin cuenta. No hay registro de ejemplo público.",
  "ej": null
 },
 {
  "id": "ap1",
  "n": "Apify · memo23 / inmuebles24-scraper",
  "tipo": "Actor de scraping (pago por resultado)",
  "c": "#a78bfa",
  "url": "https://apify.com/memo23/inmuebles24-scraper",
  "costo": "Desde 1.00 USD por 1,000 anuncios; +0.01 USD por anuncio con contacto (teléfono/WhatsApp/correo). Plan gratis: 100 resultados por corrida.",
  "acceso": "Cuenta de Apify y token. Entrada: startUrls (URLs de búsqueda o de anuncio de inmuebles24.com), maxItems (1000), fullDescription, enrichContacts, enrichEmails, proxy.",
  "entrega": "Dataset de Apify (JSON, CSV, Excel) por API o webhook.",
  "tam": "≈39–45 campos por anuncio",
  "campos": [
   [
    "postingId",
    "string"
   ],
   [
    "postingCode",
    "string"
   ],
   [
    "url",
    "string"
   ],
   [
    "title",
    "string"
   ],
   [
    "description",
    "string"
   ],
   [
    "postingType",
    "string"
   ],
   [
    "propertyType",
    "string"
   ],
   [
    "operation",
    "string"
   ],
   [
    "price",
    "number"
   ],
   [
    "currency",
    "string"
   ],
   [
    "priceFormatted",
    "string"
   ],
   [
    "expenses",
    "number|null"
   ],
   [
    "bedrooms",
    "number|null"
   ],
   [
    "bathrooms",
    "number|null"
   ],
   [
    "parking",
    "number|null"
   ],
   [
    "totalArea",
    "number|null"
   ],
   [
    "coveredArea",
    "number|null"
   ],
   [
    "antiquity",
    "number|null"
   ],
   [
    "features",
    "object"
   ],
   [
    "address",
    "string"
   ],
   [
    "neighborhood",
    "string"
   ],
   [
    "city",
    "string"
   ],
   [
    "province",
    "string"
   ],
   [
    "country",
    "string"
   ],
   [
    "latitude",
    "number"
   ],
   [
    "longitude",
    "number"
   ],
   [
    "publisherId",
    "string"
   ],
   [
    "publisherName",
    "string"
   ],
   [
    "publisherUrl",
    "string"
   ],
   [
    "whatsApp",
    "string"
   ],
   [
    "images",
    "string[]"
   ],
   [
    "mainImage",
    "string"
   ],
   [
    "hasVideos",
    "boolean"
   ],
   [
    "hasTour",
    "boolean"
   ],
   [
    "hasPlans",
    "boolean"
   ],
   [
    "modifiedDate",
    "string"
   ],
   [
    "scrapedAt",
    "string"
   ]
  ],
  "nota": "Con enrichContacts agrega phone, agencyEmail, contactName; con enrichEmails agrega contactEmail y contactWebsite. Los tipos son los que se infieren del ejemplo de la página.",
  "ej": {
   "type": "property",
   "postingId": "150660207",
   "postingCode": "66006R",
   "url": "https://www.inmuebles24.com/propiedades/desarrollo/ememhoin-reserva-constanza-150660207.html",
   "title": "Reserva Constanza",
   "description": "Descubre reserva constanza: Tu nuevo hogar en Santiago, Nuevo León. Imagina vivir en una casa que combine elegancia, ...",
   "postingType": "DEVELOPMENT",
   "propertyType": "Desarrollos horizontales",
   "operation": "Venta",
   "price": 5600000,
   "currency": "MN",
   "priceFormatted": "5,600,000",
   "expenses": null,
   "bedrooms": null,
   "bathrooms": null,
   "parking": null,
   "totalArea": null,
   "coveredArea": null,
   "antiquity": null,
   "features": {},
   "address": "Huertas, Las Huertas",
   "neighborhood": "Los Cavazos",
   "city": "Santiago",
   "province": "Nuevo León",
   "country": "Mexico"
  }
 },
 {
  "id": "ap2",
  "n": "Apify · scrapers_lat / vivanuncios-mexico-scraper",
  "tipo": "Actor de scraping (pago por resultado)",
  "c": "#f472b6",
  "url": "https://apify.com/scrapers_lat/vivanuncios-mexico-scraper/api",
  "costo": "Desde 17.00 USD por 1,000 resultados; solo se cobra lo que devuelve. Plan gratis: 10 registros por corrida.",
  "acceso": "Cuenta de Apify y token. Entrada: searchUrls (obligatorio, URLs de vivanuncios.com.mx), maxListings (10), withDetails (agrega agencia, teléfono, WhatsApp, coordenadas, descripción, galería y amenidades), minPrice y maxPrice.",
  "entrega": "Dataset de Apify (JSON, CSV, Excel) por API o webhook.",
  "tam": "≈40 campos (los de detalle solo con withDetails)",
  "campos": [
   [
    "listingId",
    "string"
   ],
   [
    "listingUrl",
    "string"
   ],
   [
    "title",
    "string"
   ],
   [
    "priceAmount",
    "number"
   ],
   [
    "priceCurrency",
    "string (MXN|USD)"
   ],
   [
    "operation",
    "string (venta|renta)"
   ],
   [
    "propertyType",
    "string"
   ],
   [
    "isDevelopment",
    "boolean"
   ],
   [
    "bedrooms",
    "number"
   ],
   [
    "bathrooms",
    "number"
   ],
   [
    "parkingSpaces",
    "number"
   ],
   [
    "totalAreaM2",
    "number"
   ],
   [
    "builtAreaM2",
    "number (detalle)"
   ],
   [
    "pricePerM2",
    "number"
   ],
   [
    "location",
    "string"
   ],
   [
    "colonia",
    "string"
   ],
   [
    "city",
    "string"
   ],
   [
    "state",
    "string|null"
   ],
   [
    "latitude",
    "number (detalle)"
   ],
   [
    "longitude",
    "number (detalle)"
   ],
   [
    "advertiserName",
    "string (detalle)"
   ],
   [
    "agencyName",
    "string (detalle)"
   ],
   [
    "phone",
    "string (detalle)"
   ],
   [
    "whatsapp",
    "string (detalle)"
   ],
   [
    "description",
    "string (detalle)"
   ],
   [
    "propertyAge",
    "number (detalle)"
   ],
   [
    "amenities",
    "string[] (detalle)"
   ],
   [
    "amenitiesCount",
    "number"
   ],
   [
    "imageUrl",
    "string"
   ],
   [
    "imageUrls",
    "string[] (detalle)"
   ],
   [
    "publishedAt",
    "string (detalle)"
   ],
   [
    "observedAt",
    "string"
   ]
  ],
  "nota": "Sin withDetails no trae coordenadas, fotos completas ni contacto. El ejemplo de la página es de Cancún; el catálogo cubre todo México.",
  "ej": {
   "listingId": "145641919",
   "listingUrl": "https://www.vivanuncios.com.mx/a-venta-departamento/supermanzana-11/…/145641919",
   "title": "Departamento de 3 habitaciones, Cancún · PH en Renta/venta en Palmar Residencial Cancún Sm 12",
   "priceAmount": 8500000,
   "priceCurrency": "MXN",
   "operation": "venta",
   "propertyType": "Departamento",
   "isDevelopment": false,
   "bedrooms": 3,
   "bathrooms": 3,
   "parkingSpaces": 2,
   "totalAreaM2": 310,
   "pricePerM2": 27419,
   "location": "Supermanzana 11, Cancún",
   "colonia": "Supermanzana 11",
   "city": "Cancún",
   "state": null,
   "description": "Amplio penthouse construido en dos plantas 310 m2 entre ambas plantas Amueblado 3 recámaras 3.5 baños ...",
   "builtAreaM2": 155,
   "propertyAge": 5,
   "publishedAt": "2025-02-06T00:08:14Z",
   "amenities": [
    "Amueblado",
    "Elevador",
    "Caseta de guardia",
    "Alberca",
    "..."
   ],
   "amenitiesCount": 27
  }
 },
 {
  "id": "ml",
  "n": "Mercado Libre · API de desarrolladores",
  "tipo": "API oficial",
  "c": "#eab308",
  "url": "https://developers.mercadolibre.com.mx",
  "costo": "Sin costo por consulta; requiere registrar una aplicación.",
  "acceso": "Token OAuth (Bearer). Búsqueda por sitio y categoría de inmuebles (/sites/MLM/search) y ficha por ítem (/items/{id}).",
  "entrega": "JSON por HTTP.",
  "tam": "—",
  "campos": [],
  "nota": "Sin muestra verificada: la documentación bloquea la lectura automática (error 403), y una búsqueda previa solo confirmó el patrón de la API para Argentina. Para ver la estructura real hay que registrar la app y probar una consulta.",
  "ej": null
 },
 {
  "id": "eb",
  "n": "EasyBroker · API de propiedades",
  "tipo": "API oficial (solo cuentas propias)",
  "c": "#34d399",
  "url": "https://docs.easybroker.com/",
  "costo": "Incluida con la cuenta de EasyBroker.",
  "acceso": "Clave de API de la cuenta. Sirve para leer y publicar las propiedades de esa cuenta, no para minería de mercado.",
  "entrega": "JSON por HTTP.",
  "tam": "—",
  "campos": [],
  "nota": "Sin muestra verificada: la página de documentación estaba inactiva al revisarla.",
  "ej": null
 }
];
