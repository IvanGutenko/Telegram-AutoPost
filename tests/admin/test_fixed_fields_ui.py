import test_source_onboarding_duplicates as onboarding

from emoda.db.models import Source

journey = onboarding.journey


async def test_fixed_fields_edit_disable_validation_and_isolation(journey):
    before=await journey.snapshot()
    await journey.callback('source:1')
    assert 'source:fixed:1' in journey.buttons()
    await journey.callback('source:fixed:1')
    assert 'source:fixed-set:1:sizes' in journey.buttons()
    await journey.callback('source:fixed-set:1:sizes')
    await journey.message('S M L XL XXL')
    assert 'сохранено' in journey.screens[-1][0]
    await journey.callback('source:fixed-set:1:quantity')
    await journey.message('5.5')
    assert 'целое' in journey.screens[-1][0]
    await journey.message('5')
    async with journey.sessions() as session:
        s=await session.get(Source,1)
        assert s.profile.config['fixed_fields']=={'sizes':'S M L XL XXL','quantity':'5'}
        previous={k:v for k,v in before[0][8].items() if k!='fixed_fields'}
        assert {k:v for k,v in s.profile.config.items() if k!='fixed_fields'}==previous
    await journey.callback('source:fixed-off:1:sizes')
    async with journey.sessions() as session:
        assert (await session.get(Source,1)).profile.config['fixed_fields']=={'quantity':'5'}
    await journey.callback('source:fixed-off:1:quantity')
    async with journey.sessions() as session:
        assert (await session.get(Source,1)).profile.config['fixed_fields']=={}


async def test_price_button_text_and_contact_edit_preserves_existing_buttons(journey, monkeypatch):
    from aiogram import Bot
    from aiogram.types import User

    from emoda.db.models import GlobalSetting

    async def get_me(self):
        return User(id=42,is_bot=True,first_name='Bot',username='emoda_bot')
    monkeypatch.setattr(Bot,'get_me',get_me)
    original={'order': {'text':'Order original','url':'https://wa.me/905552942994'},'catalog':{'text':'Catalog original','url':'https://example.com/catalog'}}
    async with journey.sessions() as session, session.begin():
        session.add(GlobalSetting(key='publication_buttons',value=original))
    await journey.callback('menu:buttons')
    assert 'buttons:price-text' in journey.buttons()
    await journey.callback('buttons:price-text')
    assert 'новый текст кнопки' in journey.screens[-1][0]
    assert not any('Код ошибки' in answer for answer in journey.answers)
    await journey.message('💲 Проверить цену / Price')
    await journey.callback('buttons:url:check_price')
    await journey.message('https://example.com')
    assert 'Некорректный URL' in journey.screens[-1][0]
    await journey.message('https://wa.me/12345678900')
    async with journey.sessions() as session:
        value=(await session.get(GlobalSetting,'publication_buttons')).value
        assert value['order']==original['order'] and value['catalog']==original['catalog']
        assert value['check_price']=={'text':'💲 Проверить цену / Price','url':'https://wa.me/12345678900'}
    await journey.callback('buttons:url:check_price')
    await journey.message('-')
    async with journey.sessions() as session:
        assert (await session.get(GlobalSetting,'publication_buttons')).value['check_price']['url'] is None
