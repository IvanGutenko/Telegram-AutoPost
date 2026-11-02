from urllib.parse import parse_qs, urlsplit

import pytest

from emoda.services.publication import publication_keyboard


@pytest.mark.parametrize('contact', [
    'https://wa.me/905552942994',
    'https://wa.me/905552942994?text=old',
    'https://api.whatsapp.com/send?phone=905552942994&app_absent=0&text=old',
    'https://t.me/emoda_orders',
    'https://t.me/emoda_orders?text=old',
    'https://telegram.me/emoda_orders',
    'https://t.me/+905552942994',
])
def test_order_button_carries_its_product_code_to_the_configured_contact(contact):
    settings = {
        'order': {'text': '🛒 Заказать / Order', 'url': contact},
        'catalog': {'text': '📂 Каталог / Catalog', 'url': 'https://example.com/catalog'},
    }
    before = {key: dict(value) for key, value in settings.items()}
    result = publication_keyboard(settings, bot_username='emoda_bot', emoda_code='AA12345')
    assert len(result) == 3 and all(len(row) == 1 for row in result)
    order = result[0][0]
    assert order['text'] == settings['order']['text']
    assert urlsplit(order['url'])[:3] == urlsplit(contact)[:3]
    query = parse_qs(urlsplit(order['url']).query)
    assert query['text'] == [
        'Здравствуйте! Хочу заказать товар E-MODA AA12345.\n'
        'Hello! I would like to order E-MODA AA12345.'
    ]
    if 'phone' in parse_qs(urlsplit(contact).query):
        assert query['phone'] == ['905552942994']
        assert query['app_absent'] == ['0']
    assert result[1][0] == settings['catalog']
    price_query = parse_qs(urlsplit(result[2][0]['url']).query)
    assert price_query['text'] == [
        'Здравствуйте! Уточните цену товара E-MODA AA12345.\n'
        'Hello! Please check the price of E-MODA AA12345.'
    ]
    assert settings == before


def test_order_and_price_keep_separate_contacts_and_codes_never_leak_between_products():
    settings = {
        'order': {'text': 'Buy', 'url': 'https://wa.me/12345678900'},
        'check_price': {'text': 'Price', 'url': 'https://t.me/emoda_manager'},
    }
    first = publication_keyboard(settings, bot_username='emoda_bot', emoda_code='AA00001')
    second = publication_keyboard(settings, bot_username='emoda_bot', emoda_code='AA00002')
    for result, code, other in [(first, 'AA00001', 'AA00002'), (second, 'AA00002', 'AA00001')]:
        assert urlsplit(result[0][0]['url']).path == '/12345678900'
        assert urlsplit(result[2][0]['url']).path == '/emoda_manager'
        for index in (0, 2):
            draft = parse_qs(urlsplit(result[index][0]['url']).query)['text'][0]
            assert code in draft and other not in draft
    assert first[1] == second[1]


def test_order_code_is_url_encoded_without_creating_query_parameters():
    code = 'G045 LP4002 &?#/Б'
    result = publication_keyboard(None, bot_username='emoda_bot', emoda_code=code)
    query = parse_qs(urlsplit(result[0][0]['url']).query)
    assert set(query) == {'text'}
    assert query['text'][0].count(code) == 2
    assert not urlsplit(result[0][0]['url']).fragment


def test_legacy_external_order_link_keeps_its_contact_without_guessing_a_recipient():
    contact = 'https://example.com/order?sku=123'
    result = publication_keyboard({'order': {'url': contact}}, bot_username='emoda_bot', emoda_code='AA12345')
    assert result[0][0]['url'] == contact
    assert result[2][0]['url'] == contact
