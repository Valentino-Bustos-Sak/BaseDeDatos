import reverse_geocoder as rg

def obtener_geografia_offline(lat, lon):
    resultado = rg.search([(lat, lon)])[0]   
    codigo_iso = resultado.get('cc', 'Desconocido').upper()
    region = resultado.get('admin1', 'Desconocido')
    ciudad = resultado.get('name', 'Desconocido')       
    mapeo_paises = {
            'AR': 'Argentina',
            'US': 'Estados Unidos',
            'BR': 'Brasil',
            'ES': 'España',
            'UY': 'Uruguay'
        }
    pais = mapeo_paises.get(codigo_iso, codigo_iso)
    return pais, region, ciudad, codigo_iso