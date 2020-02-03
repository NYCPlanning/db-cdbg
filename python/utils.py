from geosupport import Geosupport, GeosupportError
import usaddress
import re
import os

g = Geosupport()

def get_hnum(address):
    '''
    Cleans input address and uses usaddress to create a string
    of house number information.
    '''
    address = '' if address is None else quick_clean(address)
    result = [k for (k, v) in usaddress.parse(address)
              if re.search("Address", v)] if address is not None else ''
    result = ' '.join(result)
    if result == '':
        return address
    else:
        return result


def get_sname(address):
    '''
    Cleans input address and uses usaddress to create a string
    of street name information.
    '''
    address = '' if address is None else quick_clean(address)
    result = [k for (k, v) in usaddress.parse(address)
              if re.search("Street", v)] if address is not None else ''
    result = ' '.join(result)
    if address == '':
        return address
    else:
        return result

def quick_clean(address):
    '''
    Strips accents and spaces padding hyphenated addresses, then uses a ML-based
    classification to parse into portions. These portions are then stripped
    of punctuation and returned as a single string.
    '''
    # Parse with usaddress, then remove portions that are unit- or recipient-specific
    address = '-'.join([i.strip() for i in str(address).split('-')]
                       ) if address is not None else ''
    result = [k for (k, v) in usaddress.parse(address)
              if not v in
              ['OccupancyIdentifier', 'OccupancyType', 'Recipient']]
    # Join results as a single string, replacing punctuation with spaces
    return re.sub(r'[,\%\$\#\@\!\_\.\?\`\"\(\)\ï\¿\½\’]', '', ' '.join(result))

def get_sname(b5sc): 
    try:
        geo = g['D'](B5SC=b5sc)
        return geo.get('First Street Name Normalized', '')
    except: 
        return ''

def get_address(bbl):
    try:
        geo = g['BL'](bbl=bbl)
        addresses = geo.get('LIST OF GEOGRAPHIC IDENTIFIERS', '')
        filter_addresses = [d for d in addresses if d['Low House Number'] != '' and d['5-Digit Street Code'] != '']
        address = filter_addresses[0]
        b5sc = address.get('Borough Code', '0')+address.get('5-Digit Street Code', '00000')
        sname = get_sname(b5sc)
        hnum = address.get('Low House Number', '')
        return dict(sname=sname, hnum=hnum)
    except:
        return dict(sname='', hnum='')

def geocode_address(inputs):
    hnum = inputs.get('hnum', '')
    sname = inputs.get('sname', '')
    borough = inputs.get('boro', '')

    hnum = str('' if hnum is None else hnum)
    sname = str('' if sname is None else sname)
    borough = str('' if borough is None else borough)

    try:
        geo = g['1B'](street_name=sname, house_number=hnum, borough=borough)
        geo = parse_output(geo)
        geo.update({'status':'success'})
    except:
        try:
            geo = g['1B'](street_name=f'{hnum} {sname}', house_number='', borough=borough)
            geo = parse_output(geo)
            geo.update({'status':'success'})
        except GeosupportError as e:
            geo = e.result
            geo = parse_output(geo)
            geo.update({'status':'failure'})

    geo.update(inputs)
    return geo

def geocode_bbl(inputs):
    bbl=inputs.get('bbl', '')
    borough = inputs.get('boro', '')
    address = get_address(bbl)
    sname = address.get('sname', '')
    hnum = address.get('hnum', '')

    try: 
        geo = g['1B'](street_name=sname, house_number=hnum, borough=borough, mode='regular')
        geo = parse_output(geo)
        geo.update(inputs)
        geo.update({'status':'success'})
        return geo
    except GeosupportError: 
        try: 
            geo = g['1B'](street_name=sname, house_number=hnum, borough=borough, mode='tpad')
            geo = parse_output(geo)
            geo.update(inputs)
            geo.update({'status':'success'})
            return geo
        except GeosupportError as e1:
            try:
                geo = g['BL'](bbl=bbl)
                geo = parse_output(geo)
                geo.update(inputs)
                geo.update({'status':'success'})
                return geo
            except GeosupportError as e2:
                geo = parse_output(e1.result)
                geo.update(inputs)
                geo.update({'status':'failure'})
                return geo

def parse_output(geo):
    '''
    Parses nested dictionary output by geosupport to extract coordinate
    information, error codes, and messages.
    '''
    geo = dict(
        geo_house_number=geo.get('House Number - Display Format', ''),
        geo_street_name=geo.get('First Street Name Normalized', ''),
        geo_address=geo.get('House Number - Display Format', '')+' '+geo.get('First Street Name Normalized', ''),
        geo_borough_code=geo.get(
            'BOROUGH BLOCK LOT (BBL)', {}).get('Borough Code', ''),
        geo_zip_code=geo.get('ZIP Code', ''),
        geo_bin=geo.get(
            'Building Identification Number (BIN) of Input Address or NAP', ''),
        geo_bbl=geo.get('BOROUGH BLOCK LOT (BBL)', {}).get(
            'BOROUGH BLOCK LOT (BBL)', '',),
        geo_block = geo.get('BOROUGH BLOCK LOT (BBL)', {}).get('Tax Block', ''),
        geo_lot = geo.get('BOROUGH BLOCK LOT (BBL)', {}).get('Tax Lot', ''),
        geo_stcode = geo.get('B10SC - First Borough and Street Code', '')[1:6],
        geo_latitude=geo.get('Latitude', ''),
        geo_longitude= geo.get('Longitude', ''),
        geo_city=geo.get('USPS Preferred City Name', ''),
        geo_xy_coord=geo.get('Spatial X-Y Coordinates of Address', ''),
        geo_commboard=geo.get('COMMUNITY DISTRICT', {}).get(
            'COMMUNITY DISTRICT', ''),
        geo_cong=geo.get('Congressional District', ''),
        geo_nta=geo.get('Neighborhood Tabulation Area (NTA)', ''),
        geo_council=geo.get('City Council District', ''),
        geo_censtract=geo.get('2010 Census Tract', ''),
        geo_grc=geo.get('Geosupport Return Code (GRC)', ''),
        geo_grc2=geo.get('Geosupport Return Code 2 (GRC 2)', ''),
        geo_reason_code=geo.get('Reason Code', ''),
        geo_message=geo.get('Message', 'msg err'),
        geo_policeprct=geo.get('Police Precinct', ''),
        geo_schooldist=geo.get('Community School District', ''),
    )
    return geo