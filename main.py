#!/usr/bin/env python
# encoding: utf8

import sys
import re
import os.path
import json
import cjson
from random import sample, random
from hashlib import sha1
from collections import namedtuple, Counter

import requests
requests.packages.urllib3.disable_warnings()

import seaborn as sns
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib import rc
import matplotlib.ticker as mtick
# For cyrillic labels
rc('font', family='Verdana', weight='normal')

import pandas as pd
from bs4 import BeautifulSoup


DATA_DIR = 'data'
HTML_DIR = os.path.join(DATA_DIR, 'html')
HTML_LIST = os.path.join(HTML_DIR, 'list.txt')
ROOT_URL = 'https://www.reformagkh.ru/myhouse?geo=reset'
REGIONS = os.path.join(DATA_DIR, 'regions.json')
REGION_LISTS = os.path.join(DATA_DIR, 'region_lists.json')
PROFILES_DIR = os.path.join(DATA_DIR, 'profiles')
BLUE = '#4a71b2'
DATA = 'data.csv'


RegionRecord = namedtuple(
    'RegionRecord',
    ['parent', 'name', 'id', 'buildings']
)
RegionListRecord = namedtuple(
    'RegionListRecord',
    ['region', 'id', 'address', 'year', 'area', 'company']
)
Coordinates = namedtuple('Coordinates', ['longitude', 'latitude'])
WhenBuildingWas = namedtuple(
    'WhenBuildingWas',
    ['built', 'opened']
)
BuildingFloors = namedtuple('BuildingFloors', ['min', 'max'])
BuildingMeasures = namedtuple(
    'BuildingMeasures',
    ['floors', 'appartments', 'entrances', 'elevators',
     'area_meters', 'parking_meters']
)
BuildingType = namedtuple(
    'BuildingType',
    ['building', 'series', 'capital', 'repair', 'energy']
)
BuildingProfile = namedtuple(
    'BuildingProfile',
    ['region', 'id', 'coordinates', 'when_was', 'number_of', 'type_of']
)


def log_progress(sequence, every=None, size=None):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = size / 200     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{index} / ?'.format(index=index)
                else:
                    progress.value = index
                    label.value = u'{index} / {size}'.format(
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = str(index or '?')


def jobs_manager():
    from IPython.lib.backgroundjobs import BackgroundJobManager
    from IPython.core.magic import register_line_magic
    from IPython import get_ipython
    
    jobs = BackgroundJobManager()

    @register_line_magic
    def job(line):
        ip = get_ipython()
        jobs.new(line, ip.user_global_ns)

    return jobs


def kill_thread(thread):
    import ctypes
    
    id = thread.ident
    code = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(id),
        ctypes.py_object(SystemError)
    )
    if code == 0:
        raise ValueError('invalid thread id')
    elif code != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(id),
            ctypes.c_long(0)
        )
        raise SystemError('PyThreadState_SetAsyncExc failed')


def get_chunks(sequence, count):
    count = min(count, len(sequence))
    chunks = [[] for _ in range(count)]
    for index, item in enumerate(sequence):
        chunks[index % count].append(item) 
    return chunks

def hash_url(url):
    return sha1(url.encode('utf8')).hexdigest()


def get_html_filename(url):
    return '{hash}.html'.format(
        hash=hash_url(url)
    )


def get_html_path(url):
    return os.path.join(
        HTML_DIR,
        get_html_filename(url)
    )


def list_urls_cache(path):
    with open(path) as file:
        for line in file:
            line = line.decode('utf8').strip()
            hash, url = line.split('\t', 1)
            yield url


def list_html_cache():
    return list_urls_cache(HTML_LIST)


def filter_banned(urls):
    size = get_file_size(BAN)
    html = load_text(BAN)
    for url in urls:
        path = get_html_path(url)
        if get_file_size(path) == size and load_text(path) == html:
            yield url


def update_urls_cache(url, path):
    with open(path, 'a') as file:
        hash = hash_url(url)
        file.write('{hash}\t{url}\n'.format(
            hash=hash,
            url=url.encode('utf8')
        ))


def update_html_cache(url):
    update_urls_cache(url, HTML_LIST)


def dump_html(url, html):
    path = get_html_path(url)
    if html is None:
        html = ''
    with open(path, 'w') as file:
        file.write(html.encode('utf8'))
    update_html_cache(url)


def load_text(path):
    with open(path) as file:
        return file.read().decode('utf8')


def load_html(url):
    path = get_html_path(url)
    return load_text(path)


def curl_url(url):
    try:
        response = requests.get(
            url,
            headers={
                'User-Agent': ('Mozilla/5.0 (Windows NT 6.3; Win64; x64) '
                               'AppleWebKit/537.36 (KHTML, like Gecko) '
                               'Chrome/37.0.2049.0 Safari/537.36')
            },
            timeout=100
        )
        return response.text
    except requests.RequestException:
        return None


def fetch_url(url):
    html = curl_url(url)
    dump_html(url, html)


def fetch_urls(urls):
    for url in urls:
        fetch_url(url)


def get_soup(html):
    return BeautifulSoup(html, 'lxml')


def parse_reforma_int(string):
    return int(string.replace(' ', ''))


def parse_regions_list(html, parent=None):
    soup = get_soup(html)
    for item in soup.find_all('tr', class_='left'):
        link = item.find('a')
        if link:
            tid = None
            if link.has_attr('href'):  # if item is disampled tag is a
                                       # but has no href
                tid = int(re.search('tid=(\d+)', link['href']).group(1))
            name = link.text
            next = item.find_next_sibling()
            buildings = parse_reforma_int(next.find('span').text)
            yield RegionRecord(parent, name, tid, buildings)


def subregions_list_url(id):
    if id:
        return 'https://www.reformagkh.ru/myhouse?tid={id}'.format(
            id=id
        )


def load_raw_regions():
    html = load_html(ROOT_URL)
    regions = parse_regions_list(html)
    for region in regions:
        yield region


def load_raw_subregions(regions):
    for region in regions:
        id = region.id
        if id is None:
            yield region
        else:
            url = subregions_list_url(id)
            html = load_html(url)
            subregions = list(parse_regions_list(html, parent=region))
            if not subregions:
                yield region
            else:
                for subregion in subregions:
                    yield subregion


def load_json_data(path):
    with open(path) as file:
        return cjson.decode(file.read())
    

def dump_json_data(data, path):
    with open(path, 'w') as file:
        return json.dump(data, file)


def dump_regions(regions):
    parents = {}
    leafs = []
    for region in regions:
        parent, name, id, buildings = region
        leafs.append((parent.id, name, id, buildings))
        while parent:
            parent, name, id, buildings = parent
            parents[id] = (
                parent.id if parent else None,
                name,
                buildings
            )
    dump_json_data((parents, leafs), REGIONS)


def load_parent_region(id, parents):
    parent_id, name, buildings = parents[id]
    if parent_id is None:
        parent = None
    else:
        parent = load_parent_region(parent_id, parents)
    # NOTE New memory for every parent
    return RegionRecord(parent, name, id, buildings)


def load_regions():
    parents, leafs = load_json_data(REGIONS)
    parents = {int(key): value for key, value in parents.iteritems()}
    for parent_id, name, id, buildings in leafs:
        yield RegionRecord(
            load_parent_region(parent_id, parents),
            name, id, buildings
        )


def region_list_url(id):
    if id:
        # 100 000 is enough. Samara is top and has ~12 000
        return ('https://www.reformagkh.ru/myhouse'
                '?tid={id}&page=1&limit=100000').format(
            id=id
        )

def parse_reforma_float(string):
    return float(string.replace(' ', ''))


def parse_region_list(html, region=None):
    soup = get_soup(html)
    table = soup.find('div', class_='grid')
    if table is None:
        print >>sys.stderr, 'Unable to parse region:', region.id
        return
    table = table.find('table')
    for row in table.find_all('tr')[1:]:  # skip header
        address, year, area, company = row.find_all('td')
        link = address.find('a')
        id = int(re.search('^/myhouse/profile/view/(\d+)/', link['href']).group(1))
        address = link.text
        year = year.text
        if year == u'н.д.':
            year = None
        else:
            year = int(year)
        area = area.text
        if area == u'н.д.':
            area = None
        else:
            area = parse_reforma_float(area)
        company = company.text
        if company == u'Не заполнено':
            company = None
        yield RegionListRecord(region, id, address, year, area, company)
    

def load_raw_region_lists(regions):
    for region in regions:
        id = region.id
        if id is not None:
            url = region_list_url(id)
            html = load_html(url)
            for record in parse_region_list(html, region=region):
                yield record


def dump_region_lists(results):
    data = []
    companies = {}
    for company_id, (region, id, address, year, area, company) in enumerate(results):
        if company is None:
            company_id = None
        else:
            if company in companies:
                company_id = companies[company]
            else:
                companies[company] = company_id
        data.append((
            region.id,
            id,
            address,
            year,
            area,
            company_id
        ))
    dump_json_data((companies, data), REGION_LISTS)


def load_region_lists(regions):
    regions = {_.id: _ for _ in regions}
    companies, results = load_json_data(REGION_LISTS)
    companies = {id: name for name, id in companies.iteritems()}
    companies[None] = None
    for region_id, id, address, year, area, company_id in results:
        yield RegionListRecord(
            regions[region_id],
            id,
            address,
            year,
            area,
            companies[company_id]
        )


def filter_region_lists(results, name):
    for result in results:
        parent = result.region.parent.parent
        if parent and parent.name == name:
            yield result


def building_profile_url(id):
    if id:
        return 'https://www.reformagkh.ru/myhouse/profile/view/{id}/'.format(
            id=id
        )


def parse_building_profile_coordinates(html):
    match = re.search('var myPlacemark = new ymaps\.Placemark'
                      '\(\s+\[([\d\.]+),([\d\.]+)\]', html)
    if match:
        longitude, latitude = match.groups()
        longitude = float(longitude)
        latitude = float(latitude)
        return Coordinates(longitude, latitude)


def int_or_none(value, int=int):
    if value is not None:
        return int(value)


def parse_building_profile_dates(data):
    built = int_or_none(data.get(u'Год постройки'))
    opened = int_or_none(data.get(u'Год ввода дома в эксплуатацию'))
    return WhenBuildingWas(built, opened)


def parse_building_profile_measures(data):
    min_floors = int_or_none(
        data.get(u'наименьшее, ед.'),
        int=parse_reforma_int
    )
    max_floors = int_or_none(
        data.get(u'наибольшее, ед.'),
        int=parse_reforma_int
    )
    appartments = int_or_none(
        data.get(u'Количество помещений, в том числе:'),
        int=parse_reforma_int
    )
    entrances = int_or_none(
        data.get(u'Количество подъездов, ед.'),
        int=parse_reforma_int
    )
    elevators = int_or_none(
        data.get(u'Количество лифтов, ед.'),
        int=parse_reforma_int
    )
    area_meters = int_or_none(
        data.get(u'Общая площадь дома, кв.м'),
        int=parse_reforma_float
    )
    parking_meters = int_or_none(
        data.get(u'площадь парковки в границах земельного участка, кв.м'),
        int=parse_reforma_float
    )
    return BuildingMeasures(
        BuildingFloors(min_floors, max_floors),
        appartments, entrances, elevators,
        area_meters, parking_meters
    )
    

def parse_building_profile_types(data):
    type = data.get(u'Тип дома')
    series = data.get(u'Серия, тип постройки здания')
    capital = data.get(u'Способ формирования фонда капитального ремонта')
    repair = data.get(u'Дом признан аварийным')
    if repair == u'Да':
        repair = True
    elif repair == u'Нет':
        repair = False
    energy = data.get(u'Класс энергетической эффективности')
    if energy == u'Не присвоен':
        energy = None
    return BuildingType(
        type, series, capital,
        repair, energy
    )


def parse_building_profile_data(html):
    data = {}
    for match in re.finditer(
        r'<tr class="left".*?>(.+?)</tr>\s+<tr.*?>(.+?)</tr>', html, re.S
    ):
        key, value = match.groups()
        match = re.search(r'<span.*?>(.+?)</span>', key)
        key = match.group(1)
        match = re.search(r'<span.*?>(.+?)</span>', value, re.S)
        value = match.group(1).strip()
        if value == u'Не заполнено':
            value = None
        data[key] = value
    return data


def parse_building_profile(html, region=None, id=None):
    data = parse_building_profile_data(html)
    coordinates = parse_building_profile_coordinates(html)
    when_was = parse_building_profile_dates(data)
    number_of = parse_building_profile_measures(data)
    type_of = parse_building_profile_types(data)
    return BuildingProfile(
        region, id, coordinates,
        when_was, number_of, type_of
    )


def get_profile_filename(id):
    return '{id}.json'.format(id=id)


def get_profile_path(id):
    return os.path.join(
        PROFILES_DIR,
        get_profile_filename(id)
    )


def parse_profile_filename(filename):
    id, _ = filename.split('.', 1)
    return int(id)


def list_profiles_cache():
    for filename in os.listdir(PROFILES_DIR):
        yield parse_profile_filename(filename)


def dump_profile(profile):
    region, id, coordinates, when_was, number_of, type_of = profile
    data = [
        region.id,
        id,
        coordinates,
        when_was,
        number_of,
        type_of
    ]
    path = get_profile_path(id)
    dump_json_data(data, path)
    

def preparse_profile(result):
    id = result.id
    url = building_profile_url(id)
    html = load_html(url)
    profile = parse_building_profile(html, result.region, id)
    dump_profile(profile)


def preparse_profiles(results):
    for result in results:
        preparse_profile(result)


def load_profile(id, regions_mapping):
    path = get_profile_path(id)
    data = load_json_data(path)
    region_id, id, coordinates, when_was, number_of, type_of = data
    if coordinates is not None:
        coordinates = Coordinates(*coordinates)
    floors, appartments, entrances, elevators, area, parking = number_of
    floors = BuildingFloors(*floors)
    return BuildingProfile(
        regions_mapping[region_id],
        id,
        coordinates,
        WhenBuildingWas(*when_was),
        BuildingMeasures(floors, appartments, entrances, elevators, area, parking),
        BuildingType(*type_of)
    )


def load_profiles(results, regions):
    mapping = {_.id: _ for _ in regions}
    for result in results:
        yield load_profile(result.id, mapping)


def show_buildings_count_by_year(profiles):
    data = []
    for profile in profiles:
        year = profile.when_was.opened
        if year is not None and 1900 <= year <= 2015:
            data.append(year)
    table = pd.Series(data)
    fig, ax = plt.subplots()
    table.value_counts().sort_index().plot(ax=ax)
    ax.set_ylabel(u'число новых домов')
    fig.savefig('fig.png', dpi=300, bbox_inches='tight')


def show_floors_count_by_year(profiles):
    data = []
    xs = []
    ys = []
    for profile in sample(profiles, 100000):
        year = profile.when_was.opened
        if year is not None and 1900 <= year <= 2015:
            floors = profile.number_of.floors.max
            if floors is not None and floors <= 30:
                xs.append(year + random())
                ys.append(floors + (random() - 0.5) )
    fig, ax = plt.subplots()
    ax.scatter(xs, ys, s=1, color=BLUE, alpha=0.1)
    ax.set_ylabel(u'число этажей')
    ax.set_ylim((-2, 30))
    ax.set_xlim((1900, 2015))
    fig.savefig('fig.png', dpi=300, bbox_inches='tight')


def show_appartments_count_by_year(profiles):
    data = []
    xs = []
    ys = []
    for profile in sample(profiles, 100000):
        year = profile.when_was.opened
        if year is not None and 1900 <= year <= 2015:
            appartments = profile.number_of.appartments
            if appartments is not None and appartments <= 500:
                xs.append(year + random())
                ys.append(appartments + (random() - 0.5) )
    fig, ax = plt.subplots()
    ax.scatter(xs, ys, s=1, color=BLUE, alpha=0.1)
    ax.set_ylabel(u'число квартир')
    ax.set_ylim((-2, 200))
    ax.set_xlim((1900, 2015))
    fig.savefig('fig.png', dpi=300, bbox_inches='tight')


def dump_data(profiles):
    data = []
    cache = set()
    for profile in profiles:
        coordinates = profile.coordinates
        if coordinates and coordinates not in cache:
            cache.add(coordinates)
            longitude, latitude = coordinates
            year = profile.when_was.opened
            if not 1900 <= year <= 2015:
                year = None
            else:
                year = str(year)
            number_of = profile.number_of
            floors = number_of.floors.max
            if floors is not None:
                if floors < 5:
                    floors = '1..4'
                elif floors == 5:
                    pass
                elif floors < 9:
                    floors = '6..8'
                elif floors == 9:
                    pass
                else:
                    floors = '>9'
            appartments = number_of.appartments
            if appartments is not None:
                appartments = str(appartments)
            parking = number_of.parking_meters
            if parking is not None:
                parking = parking > 0
            type_of = profile.type_of
            data.append((
                longitude,
                latitude,
                year,
                floors,
                appartments,
                parking,
                type_of.repair,
                type_of.energy
            ))
    table = pd.DataFrame(
        sample(data, 500000),
        columns=['latitude', 'longitude', 'year', 'floors',
                 'appartments', 'parking', 'repair', 'energy'])
    table.to_csv(DATA, index=False)
