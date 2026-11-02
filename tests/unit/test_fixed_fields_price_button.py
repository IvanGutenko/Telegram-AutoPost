from urllib.parse import parse_qs, urlsplit

import pytest
from pydantic import ValidationError

from emoda.domain.enums import FieldStatus, OCRMode, Provenance
from emoda.domain.schemas import ExtractedField, GeminiExtraction, SourceProfileData
from emoda.rules.engine import RuleEngine
from emoda.services.pipeline import Pipeline, PipelineInput
from emoda.services.publication import price_inquiry_url, publication_keyboard


@pytest.mark.parametrize('text', ['', 'Sizes: XS S\nQuantity: 99', 'Sizes: S M\nSizes: L XL\nQuantity: 2\nQuantity: 9'])
async def test_constants_override_missing_found_and_ambiguous_text_and_ocr(text):
    class OCR:
        requested = []
        async def extract(self, **kwargs):
            self.requested = kwargs['missing_fields']
            return GeminiExtraction(fields={
                'sizes': ExtractedField(status=FieldStatus.FOUND, value='XS', evidence='XS', provenance=Provenance.GEMINI_OCR),
                'quantity': ExtractedField(status=FieldStatus.AMBIGUOUS, evidence='99 or 100', provenance=Provenance.GEMINI_OCR),
            })
    profile = SourceProfileData(fixed_fields={'sizes': 'S M L XL XXL', 'quantity': '5'}, gemini_enabled=True, ocr_mode=OCRMode.ALWAYS)
    ocr = OCR()
    result = await Pipeline(ocr).run(PipelineInput(source_id=1,text=text,media_paths=['photo.jpg'],emoda_code='AA00001',profile=profile,markup_rules=[]),dry_run=False)
    for field, expected in profile.fixed_fields.items():
        value = result.parsed.fields[field]
        assert value.status is FieldStatus.FOUND
        assert value.value == expected
        assert value.metadata['fixed'] is True
        assert result.field_diagnostics[field].rule_id == f'{field}:fixed'
        assert field not in ocr.requested
    assert result.publication_requested
    assert not result.needs_review
    assert 'Sizes:</b> S M L XL XXL' in result.rendered_text
    assert 'Pcs per lot:</b> 5' in result.rendered_text


def test_constants_isolation_disable_and_no_quantity_inference():
    configured = SourceProfileData(fixed_fields={'sizes':'S M L XL XXL'})
    assert RuleEngine(configured).parse('').fields['quantity'].status is FieldStatus.MISSING
    assert RuleEngine(SourceProfileData()).parse('').fields['sizes'].status is FieldStatus.MISSING
    disabled = SourceProfileData.model_validate({**configured.model_dump(), 'fixed_fields': {}})
    assert RuleEngine(disabled).parse('').fields['sizes'].status is FieldStatus.MISSING


@pytest.mark.parametrize('fixed', [{'price':'1'}, {'sizes':'  '}, {'sizes':'S\nM'}, {'quantity':'0'}, {'quantity':'-5'}, {'quantity':'5.5'}, {'quantity':'five'}])
def test_invalid_constants_rejected(fixed):
    with pytest.raises(ValidationError):
        SourceProfileData(fixed_fields=fixed)


@pytest.mark.parametrize('base', ['https://wa.me/905552942994', 'https://t.me/emoda_orders', 'https://api.whatsapp.com/send?phone=905552942994'])
def test_price_button_separate_with_code_and_other_buttons_unchanged(base):
    stored={'order':{'text':'ORDER','url':base},'catalog':{'text':'CATALOG','url':'https://example.com/catalog'}}
    result=publication_keyboard(stored,bot_username='emoda_bot',emoda_code='AA12345')
    assert result[0][0]['text'] == 'ORDER'
    assert urlsplit(result[0][0]['url'])[:3] == urlsplit(base)[:3]
    assert 'AA12345' in parse_qs(urlsplit(result[0][0]['url']).query)['text'][0]
    assert result[1] == [{'text':'CATALOG','url':'https://example.com/catalog'}]
    assert len(result)==3
    assert result[2][0]['text']=='💲 Уточнить цену / Check Price'
    assert 'AA12345' in parse_qs(urlsplit(result[2][0]['url']).query)['text'][0]


def test_price_settings_independent_of_order_and_existing_query_is_preserved():
    stored={'order':{'url':'https://wa.me/905552942994'},'check_price':{'text':'PRICE?','url':'https://t.me/emoda_orders?text=old'}}
    result=publication_keyboard(stored,bot_username='emoda_bot',emoda_code='AA99999')
    assert urlsplit(result[0][0]['url']).path == '/905552942994'
    assert 'AA99999' in parse_qs(urlsplit(result[0][0]['url']).query)['text'][0]
    assert result[2][0]['text']=='PRICE?'
    assert parse_qs(urlsplit(result[2][0]['url']).query)['text'][0].count('AA99999') == 2


@pytest.mark.parametrize('url',['https://t.me/+privateinvite','https://t.me/share/url','https://example.com','https://t.me/somebot?start=catalog'])
def test_price_rejects_links_that_cannot_deliver_a_product_draft(url):
    with pytest.raises(ValueError):
        price_inquiry_url(url,'AA12345')


def test_unknown_order_url_never_routes_inquiry_to_unrelated_default_contact():
    result = publication_keyboard({'order': {'url': 'https://example.com/contact'}}, bot_username='emoda_bot', emoda_code='AA12345')
    assert result[2][0]['url'] == 'https://example.com/contact'


def test_fixed_values_win_over_computed_rules_fallbacks_and_passthrough():
    from emoda.domain.schemas import ComputedRule, SourceFallbackRule
    profile = SourceProfileData(
        fixed_fields={'sizes':'S M L XL XXL','quantity':'5'},
        field_policies={'sizes':{'processing_mode':'passthrough'}},
        computed_rules=[ComputedRule(name='count',target_field='quantity',operation='count_items',source_field='sizes')],
        fallback_rules={'quantity':SourceFallbackRule(value='77',authoritative=True)},
    )
    parsed = RuleEngine(profile).parse('Sizes: XS S\nPrice: 10 USD')
    assert parsed.fields['sizes'].value == 'S M L XL XXL'
    assert parsed.fields['quantity'].value == '5'
    assert parsed.fields['quantity'].metadata['fixed']


async def test_fixed_sizes_are_html_escaped_in_the_unchanged_renderer():
    result = await Pipeline(None).run(PipelineInput(source_id=1,text='',media_paths=['photo.jpg'],emoda_code='AA12345',profile=SourceProfileData(fixed_fields={'sizes':'S <M&L>','quantity':'5'}),markup_rules=[]),dry_run=True)
    assert 'S &lt;M&amp;L&gt;' in result.rendered_text
    assert '<M&L>' not in result.rendered_text
    assert len(result.rendered_text.splitlines()) == 4
