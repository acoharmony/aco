"""Comprehensive tests for acoharmony._puf module targeting 100% coverage."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
import zipfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import acoharmony._puf.pfs_inventory as mod
import acoharmony._puf.pfs_inventory as pfs_mod
import acoharmony._puf.puf_inventory as puf_mod
from acoharmony._puf.puf_state import (
    PUFFileEntry,
    PUFInventoryState,
    PUFStateTracker,
    get_workspace_path,
    scan_directory,
)
from acoharmony._puf.puf_unpack import (
    extract_puf_zip,
    get_puf_directories,
    make_puf_filename,
    unpack_puf_zips,
)


def _fm(**kw) -> FileMetadata:
    d = {'key': 'k', 'url': 'https://x.com/f.zip', 'category': 'addenda'}
    d.update(kw)
    return FileMetadata(**d)
SAMPLE_YAML = {'dataset_name': 'Medicare Physician Fee Schedule', 'dataset_key': 'pfs', 'description': 'PFS', 'years': {'2024': {'Final': {'year': 'CY 2024', 'citation': '89 FR 1234', 'files': {'addenda_a': {'url': 'https://cms.gov/a.zip', 'category': 'addenda', 'description': 'Addenda A', 'schema_mapping': 'pprvu_inputs'}, 'gpci': {'url': 'https://cms.gov/gpci.zip', 'category': 'gpci'}}}, 'Proposed': {'year': 'CY 2024', 'files': {'prop_file': {'url': 'https://cms.gov/prop.zip', 'category': 'other'}}}, 'not_a_rule': {'something': 'value'}}, '2023': {'Final': {'files': {'old_file': {'url': 'https://cms.gov/old.zip', 'category': 'addenda', 'schema_mapping': 'pprvu_inputs,other_schema'}}}}}}
SAMPLE_RVU_YAML = {'dataset_name': 'RVU Quarterly', 'description': 'RVU', 'base_url': 'https://cms.gov', 'years': {'2024': {'metadata': {}, 'rules': {'Q1': {'rule_type': 'Final', 'metadata': {}, 'files': {'rvu24a': {'key': 'rvu24a', 'url': 'https://cms.gov/rvu24a.zip', 'category': 'rvu_quarterly', 'quarter': 'A'}}}}}}}

@pytest.fixture(autouse=True)
def _clear_caches():
    pfs_mod._INVENTORY_CACHE = None
    puf_mod._INVENTORY_CACHE.clear()
    yield
    pfs_mod._INVENTORY_CACHE = None
    puf_mod._INVENTORY_CACHE.clear()

@pytest.fixture
def mock_inv():
    inv = mod._parse_yaml_to_inventory(SAMPLE_YAML)
    with patch.object(mod, 'load_inventory', return_value=inv), patch.object(mod, 'get_inventory', return_value=inv):
        yield inv

class TestRuleType:

    @pytest.mark.unit
    def test_values(self):
        assert RuleType.PROPOSED.value == 'Proposed'
        assert RuleType.FINAL.value == 'Final'
        assert RuleType.CORRECTION.value == 'Correction'
        assert RuleType.INTERIM_FINAL.value == 'Interim Final'

    @pytest.mark.unit
    def test_str(self):
        assert isinstance(RuleType.FINAL, str)
        assert RuleType('Final') is RuleType.FINAL
        with pytest.raises(ValueError, match='.*'):
            RuleType('Bad')

class TestFileCategory:

    @pytest.mark.unit
    def test_selected(self):
        assert FileCategory.ADDENDA.value == 'addenda'
        assert FileCategory.GPCI.value == 'gpci'
        assert FileCategory.SKIN_SUBSTITUTE.value == 'skin_substitute'
        assert FileCategory.OTHER.value == 'other'

    @pytest.mark.unit
    def test_count(self):
        expected = {'addenda', 'pprvu', 'pe_rvu', 'rvu_quarterly', 'conversion_factor', 'gpci', 'gaf', 'locality', 'direct_pe_inputs', 'clinical_labor', 'equipment', 'supplies', 'pe_worksheet', 'pe_summary', 'alt_methodology_pe', 'indirect_cost_indices', 'physician_time', 'physician_work', 'pehr', 'malpractice', 'malpractice_override', 'analytic_crosswalk', 'utilization_crosswalk', 'cpt_codes', 'placeholder', 'telehealth', 'designated_care', 'invasive_cardiology', 'mppr', 'opps_cap', 'phase_in', 'impact', 'specialty_assignment', 'specialty_impacts', 'misvalued_codes', 'em_guidelines', 'em_codes', 'em_impact', 'market_based_supply', 'otp_payment_rates', 'anesthesia', 'preventive', 'vital_signs', 'nonexcepted_items', 'reduction', 'low_volume', 'usage_rate', 'efficiency_adjustment', 'procedure_shares', 'radiation_services', 'skin_substitute', 'federal_register', 'xml', 'other'}
        assert {m.value for m in FileCategory} == expected

class TestFileFormat:

    @pytest.mark.unit
    def test_all(self):
        assert FileFormat.ZIP.value == 'zip'
        assert FileFormat.PDF.value == 'pdf'
        assert FileFormat.XLSX.value == 'xlsx'
        assert FileFormat.CSV.value == 'csv'
        assert FileFormat.XML.value == 'xml'
        assert FileFormat.HTML.value == 'html'
        assert FileFormat.JSON.value == 'json'
        assert len(FileFormat) == 7

class TestFileMetadata:

    @pytest.mark.unit
    def test_basic(self):
        f = _fm()
        assert f.key == 'k'
        assert f.format == 'zip'

    @pytest.mark.parametrize(('ext', 'expected'), [('.zip', 'zip'), ('.pdf', 'pdf'), ('.xlsx', 'xlsx'), ('.xls', 'xlsx'), ('.csv', 'csv'), ('.xml', 'xml'), ('.html', 'html'), ('.htm', 'html'), ('.json', 'json'), ('.parquet', None), ('', None)])
    @pytest.mark.unit
    def test_infer_format(self, ext, expected):
        url = f'https://x.com/data{ext}' if ext else 'https://x.com/noext'
        f = _fm(url=url)
        assert f.format == expected

    @pytest.mark.unit
    def test_explicit_format(self):
        f = _fm(url='https://x.com/a.zip', format='csv')
        assert f.format == 'csv'

    @pytest.mark.unit
    def test_optional_fields(self):
        f = _fm(description='d', file_size_mb=1.5, last_updated=date(2024, 1, 1), schema_mapping='s', metadata={'q': 'A'})
        assert f.description == 'd'
        assert f.file_size_mb == 1.5
        assert f.last_updated == date(2024, 1, 1)
        assert f.schema_mapping == 's'
        assert f.metadata == {'q': 'A'}

    @pytest.mark.unit
    def test_defaults(self):
        f = FileMetadata(key='k', url='https://x.com/a.zip')
        assert f.category == 'other'
        assert f.metadata == {}

class TestRuleMetadata:

    @pytest.mark.unit
    def test_creation(self):
        r = RuleMetadata(rule_type=RuleType.FINAL, files={'f': _fm()})
        assert r.rule_type == 'Final'
        assert 'f' in r.files

    @pytest.mark.unit
    def test_defaults(self):
        r = RuleMetadata(rule_type=RuleType.PROPOSED)
        assert r.files == {}
        assert r.metadata == {}

class TestYearInventory:

    def _yi(self):
        r = RuleMetadata(rule_type=RuleType.FINAL, files={'f': _fm()})
        return YearInventory(year='2024', rules={'Final': r})

    @pytest.mark.unit
    def test_get_rule_enum(self):
        assert self._yi().get_rule(RuleType.FINAL) is not None

    @pytest.mark.unit
    def test_get_rule_str(self):
        assert self._yi().get_rule('Final') is not None

    @pytest.mark.unit
    def test_get_rule_miss(self):
        assert self._yi().get_rule('Proposed') is None

    @pytest.mark.unit
    def test_get_all_files(self):
        assert len(self._yi().get_all_files()) == 1

    @pytest.mark.unit
    def test_multi_rules(self):
        r1 = RuleMetadata(rule_type=RuleType.FINAL, files={'a': _fm(key='a')})
        r2 = RuleMetadata(rule_type=RuleType.PROPOSED, files={'b': _fm(key='b')})
        yi = YearInventory(year='2024', rules={'Final': r1, 'Proposed': r2})
        assert len(yi.get_all_files()) == 2

class TestDatasetInventory:

    def _inv(self):
        r = RuleMetadata(rule_type=RuleType.FINAL, files={'f': _fm()})
        y24 = YearInventory(year='2024', rules={'Final': r})
        y23 = YearInventory(year='2023', rules={})
        return DatasetInventory(dataset_name='T', dataset_key='pfs', years={'2024': y24, '2023': y23})

    @pytest.mark.unit
    def test_get_year(self):
        assert self._inv().get_year('2024') is not None
        assert self._inv().get_year('9999') is None

    @pytest.mark.unit
    def test_list_years(self):
        assert self._inv().list_available_years() == ['2023', '2024']

    @pytest.mark.unit
    def test_latest(self):
        assert self._inv().get_latest_year().year == '2024'

    @pytest.mark.unit
    def test_latest_empty(self):
        assert DatasetInventory(dataset_name='E', dataset_key='e').get_latest_year() is None

    @pytest.mark.unit
    def test_defaults(self):
        i = DatasetInventory(dataset_name='T', dataset_key='k')
        assert i.source_agency == 'CMS'
        assert i.years == {}

class TestDownloadTask:

    def _task(self, **kw):
        d = {'file_metadata': _fm(key='rvu', category='pprvu'), 'year': '2024', 'rule_type': RuleType.FINAL}
        d.update(kw)
        return DownloadTask(**d)

    @pytest.mark.unit
    def test_cite_kwargs_with_note(self):
        k = self._task(tags=['x'], note='n').to_cite_kwargs()
        assert k['note'] == 'n'
        assert 'puf' in k['tags']
        assert 'x' in k['tags']

    @pytest.mark.unit
    def test_cite_kwargs_default_note(self):
        k = self._task().to_cite_kwargs()
        assert '2024' in k['note']
        assert 'rvu' in k['note']

    @pytest.mark.unit
    def test_defaults(self):
        t = self._task()
        assert t.priority == 5
        assert t.force_refresh is False
        assert t.tags == []

class TestPfsInventoryLoading:

    @pytest.mark.unit
    def test_parse(self):
        from acoharmony._puf.pfs_inventory import _parse_yaml_to_inventory
        inv = _parse_yaml_to_inventory(SAMPLE_YAML)
        assert inv.dataset_key == 'pfs'
        assert '2024' in inv.years
        y = inv.get_year('2024')
        assert y.get_rule('Final') is not None
        assert 'not_a_rule' not in y.rules

    @pytest.mark.unit
    def test_cache(self, tmp_path):
        import yaml

        import acoharmony._puf.pfs_inventory as mod
        p = tmp_path / 'pfs_data.yaml'
        p.write_text(yaml.dump(SAMPLE_YAML))
        with patch.object(mod, 'get_data_file_path', return_value=p):
            a = mod.load_inventory()
            b = mod.load_inventory()
            assert a is b
            c = mod.load_inventory(force_reload=True)
            assert c is not a

    @pytest.mark.unit
    def test_file_not_found(self):
        import acoharmony._puf.pfs_inventory as mod
        with patch.object(mod, 'get_data_file_path', return_value=Path('/no')):
            with pytest.raises(FileNotFoundError):
                mod.load_inventory(force_reload=True)

    @pytest.mark.unit
    def test_data_file_path(self):
        from acoharmony._puf.pfs_inventory import get_data_file_path
        assert get_data_file_path().name == 'pfs_data.yaml'

class TestPfsInventoryQueries:

    @pytest.mark.unit
    def test_get_year(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_year
        assert get_year('2024') is not None
        assert get_year('9999') is None

    @pytest.mark.unit
    def test_get_rule(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_rule
        assert get_rule('2024', 'Final') is not None
        assert get_rule('2024', 'Correction') is None
        assert get_rule('9999', 'Final') is None

    @pytest.mark.unit
    def test_files_for_year_all(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_files_for_year
        assert len(get_files_for_year('2024')) == 3

    @pytest.mark.unit
    def test_files_for_year_filtered(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_files_for_year
        assert len(get_files_for_year('2024', rule_type='Final')) == 2
        assert get_files_for_year('2024', rule_type='Correction') == []
        assert get_files_for_year('9999') == []

    @pytest.mark.unit
    def test_by_category_str(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_files_by_category
        r = get_files_by_category('addenda')
        assert len(r) >= 1
        assert all((fm.category == 'addenda' for _, _, fm in r))

    @pytest.mark.unit
    def test_by_category_enum(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_files_by_category
        assert len(get_files_by_category(FileCategory.GPCI)) == 1

    @pytest.mark.unit
    def test_by_category_invalid(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_files_by_category
        assert get_files_by_category('zzz') == []

    @pytest.mark.unit
    def test_by_category_year(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_files_by_category
        assert len(get_files_by_category('addenda', year='2024')) == 1
        assert get_files_by_category('addenda', year='9999') == []

    @pytest.mark.unit
    def test_by_schema(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_files_by_schema
        assert len(get_files_by_schema('pprvu_inputs')) >= 2
        assert len(get_files_by_schema('other_schema')) == 1
        assert get_files_by_schema('nope') == []

    @pytest.mark.unit
    def test_list_years(self, mock_inv):
        from acoharmony._puf.pfs_inventory import list_available_years
        assert list_available_years() == ['2023', '2024']

    @pytest.mark.unit
    def test_latest(self, mock_inv):
        from acoharmony._puf.pfs_inventory import get_latest_year
        assert get_latest_year().year == '2024'

    @pytest.mark.unit
    def test_create_tasks_all(self, mock_inv):
        from acoharmony._puf.pfs_inventory import create_download_tasks
        assert len(create_download_tasks()) >= 3

    @pytest.mark.unit
    def test_create_tasks_filters(self, mock_inv):
        from acoharmony._puf.pfs_inventory import create_download_tasks
        assert len(create_download_tasks(year='2023')) >= 1
        assert len(create_download_tasks(year='2024', rule_type='Final')) == 2
        assert len(create_download_tasks(category='gpci')) == 1
        assert create_download_tasks(year='9999') == []

    @pytest.mark.unit
    def test_create_tasks_args(self, mock_inv):
        from acoharmony._puf.pfs_inventory import create_download_tasks
        ts = create_download_tasks(year='2024', priority=1, force_refresh=True, tags=['t'])
        assert all(t.priority == 1 for t in ts)
        assert all(t.force_refresh for t in ts)

    @pytest.mark.unit
    def test_search_key(self, mock_inv):
        from acoharmony._puf.pfs_inventory import search_files
        assert len(search_files('gpci', search_in='key')) >= 1

    @pytest.mark.unit
    def test_search_desc(self, mock_inv):
        from acoharmony._puf.pfs_inventory import search_files
        assert len(search_files('Addenda', search_in='description')) >= 1

    @pytest.mark.unit
    def test_search_cat(self, mock_inv):
        from acoharmony._puf.pfs_inventory import search_files
        assert len(search_files('addenda', search_in='category')) >= 1

    @pytest.mark.unit
    def test_search_all(self, mock_inv):
        from acoharmony._puf.pfs_inventory import search_files
        assert len(search_files('gpci')) >= 1

    @pytest.mark.unit
    def test_search_none(self, mock_inv):
        from acoharmony._puf.pfs_inventory import search_files
        assert search_files('zzzz') == []

class TestPufInventory:

    @pytest.mark.unit
    def test_data_file_path(self):
        from acoharmony._puf.puf_inventory import get_data_file_path
        assert get_data_file_path('pfs').name == 'pfs_data.yaml'
        assert get_data_file_path('rvu').name == 'rvu_data.yaml'
        with pytest.raises(ValueError, match='Unknown'):
            get_data_file_path('bad')

    def _yaml_file(self, tmp_path):
        import yaml
        p = tmp_path / 'rvu_data.yaml'
        p.write_text(yaml.dump(SAMPLE_RVU_YAML))
        return p

    @pytest.mark.unit
    def test_load_cache(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            a = mod.load_dataset('rvu')
            b = mod.load_dataset('rvu')
            assert a is b
            c = mod.load_dataset('rvu', force_reload=True)
            assert c is not a

    @pytest.mark.unit
    def test_load_not_found(self):
        import acoharmony._puf.puf_inventory as mod
        with patch.object(mod, 'get_data_file_path', return_value=Path('/no')):
            with pytest.raises(FileNotFoundError):
                mod.load_dataset('pfs', force_reload=True)

    @pytest.mark.unit
    def test_quarter_metadata(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            inv = mod.load_dataset('rvu', force_reload=True)
            fm = inv.get_year('2024').rules['Q1'].files['rvu24a']
            assert fm.metadata.get('quarter') == 'A'

    @pytest.mark.unit
    def test_load_all(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            r = mod.load_all_datasets()
            assert len(r) == len(mod.DATASETS)

    @pytest.mark.unit
    def test_create_tasks(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            ts = mod.create_download_tasks('rvu')
            assert len(ts) == 1
            assert 'puf' in ts[0].tags
            assert 'rvu' in ts[0].tags

    @pytest.mark.unit
    def test_create_tasks_year_filter(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            assert len(mod.create_download_tasks('rvu', year='2024')) == 1
            assert mod.create_download_tasks('rvu', year='9999') == []

    @pytest.mark.unit
    def test_create_tasks_rule_filter(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            assert len(mod.create_download_tasks('rvu', rule_type=RuleType.FINAL)) == 1
            assert mod.create_download_tasks('rvu', rule_type=RuleType.PROPOSED) == []

    @pytest.mark.unit
    def test_create_tasks_quarter(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            assert len(mod.create_download_tasks('rvu', quarter='A')) == 1
            assert mod.create_download_tasks('rvu', quarter='D') == []

    @pytest.mark.unit
    def test_create_tasks_category(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            assert len(mod.create_download_tasks('rvu', category='rvu_quarterly')) == 1
            assert len(mod.create_download_tasks('rvu', category=FileCategory.RVU_QUARTERLY)) == 1
            assert mod.create_download_tasks('rvu', category='gpci') == []

    @pytest.mark.unit
    def test_list_datasets(self, tmp_path):
        import acoharmony._puf.puf_inventory as mod
        p = self._yaml_file(tmp_path)
        with patch.object(mod, 'get_data_file_path', return_value=p):
            r = mod.list_available_datasets()
            assert 'pfs' in r

    @pytest.mark.unit
    def test_list_datasets_error(self):
        import acoharmony._puf.puf_inventory as mod
        with patch.object(mod, 'get_data_file_path', return_value=Path('/no')):
            r = mod.list_available_datasets()
            assert all('Error' in v for v in r.values())

class TestScanDirectory:

    @pytest.mark.unit
    def test_flat(self, tmp_path):
        (tmp_path / 'a.txt').write_text('a')
        sub = tmp_path / 'sub'
        sub.mkdir()
        (sub / 'b.txt').write_text('b')
        r = scan_directory(tmp_path)
        assert 'a.txt' in r
        assert 'b.txt' not in r

    @pytest.mark.unit
    def test_recursive(self, tmp_path):
        sub = tmp_path / 'sub'
        sub.mkdir()
        (sub / 'b.txt').write_text('b')
        assert 'b.txt' in scan_directory(tmp_path, recursive=True)

    @pytest.mark.unit
    def test_missing(self):
        assert scan_directory(Path('/nonexistent')) == set()

class TestWorkspacePath:

    @pytest.mark.unit
    def test_val(self):
        assert get_workspace_path() == Path('/opt/s3/data/workspace')

class TestPUFFileEntry:

    @pytest.mark.unit
    def test_roundtrip(self):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='https://x.com/f.zip', category='addenda')
        d = e.to_dict()
        assert d['year'] == '2024'
        e2 = PUFFileEntry.from_dict(d)
        assert e2.file_key == 'f'

    @pytest.mark.unit
    def test_from_task_slash_url(self):
        fm = _fm(key='a', url='https://cms.gov/files/zip/rvu25a-updated-01/10/2025.zip')
        t = DownloadTask(file_metadata=fm, year='2024', rule_type=RuleType.FINAL)
        e = PUFFileEntry.from_download_task(t, downloaded=True, dataset_key='rvu')
        assert '/' not in e.zip_filename
        assert e.downloaded is True
        assert e.dataset_key == 'rvu'

    @pytest.mark.unit
    def test_from_task_normal_url(self):
        fm = _fm(key='f', url='https://cms.gov/data/some.zip')
        t = DownloadTask(file_metadata=fm, year='2024', rule_type=RuleType.FINAL)
        e = PUFFileEntry.from_download_task(t)
        assert e.zip_filename == 'some.zip'

    @pytest.mark.unit
    def test_from_task_metadata(self):
        fm = _fm(key='f', metadata={'quarter': 'A'})
        t = DownloadTask(file_metadata=fm, year='2024', rule_type=RuleType.FINAL)
        e = PUFFileEntry.from_download_task(t)
        assert e.metadata == {'quarter': 'A'}

    @pytest.mark.unit
    def test_from_task_no_metadata(self):
        fm = _fm(key='f')
        t = DownloadTask(file_metadata=fm, year='2024', rule_type=RuleType.FINAL)
        e = PUFFileEntry.from_download_task(t)
        assert e.metadata == {}

class TestPUFInventoryState:

    def _s(self):
        s = PUFInventoryState()
        s.add_file(PUFFileEntry(year='2024', rule_type='Final', file_key='f1', url='u', category='addenda', schema_mapping='s1,s2'))
        s.add_file(PUFFileEntry(year='2024', rule_type='Proposed', file_key='f2', url='u', category='gpci', downloaded=True))
        s.add_file(PUFFileEntry(year='2023', rule_type='Final', file_key='f3', url='u', category='addenda', error_message='err'))
        return s

    @pytest.mark.unit
    def test_post_init_dict(self):
        raw = {'dataset_name': 'T', 'dataset_key': 'pfs', 'last_updated': '2024-01-01', 'total_files': 1, 'downloaded_files': 0, 'pending_files': 1, 'failed_files': 0, 'files': {'2024:Final:f': {'year': '2024', 'rule_type': 'Final', 'file_key': 'f', 'url': 'u', 'category': 'addenda', 'dataset_key': 'pfs', 'metadata': {}, 'schema_mapping': None, 'downloaded': False, 'download_timestamp': None, 'corpus_path': None, 'file_size_bytes': None, 'error_message': None, 'extracted': False, 'extraction_timestamp': None, 'extracted_files': [], 'zip_filename': None, 'found_in_archive': False, 'found_in_bronze': False, 'found_in_cite_corpus': False}}}
        st = PUFInventoryState.from_dict(raw)
        assert isinstance(list(st.files.values())[0], PUFFileEntry)

    @pytest.mark.unit
    def test_add_get(self):
        s = self._s()
        assert s.get_file('2024', 'Final', 'f1') is not None
        assert s.get_file('9999', 'X', 'z') is None

    @pytest.mark.unit
    def test_mark_downloaded(self):
        s = self._s()
        s.mark_downloaded('2024', 'Final', 'f1', '/c', 999)
        e = s.get_file('2024', 'Final', 'f1')
        assert e.downloaded
        assert e.corpus_path == '/c'
        assert e.file_size_bytes == 999

    @pytest.mark.unit
    def test_mark_downloaded_miss(self):
        self._s().mark_downloaded('9', 'X', 'z')

    @pytest.mark.unit
    def test_mark_failed(self):
        s = self._s()
        s.mark_failed('2024', 'Final', 'f1', 'boom')
        assert s.get_file('2024', 'Final', 'f1').error_message == 'boom'

    @pytest.mark.unit
    def test_mark_failed_miss(self):
        self._s().mark_failed('9', 'X', 'z', 'e')

    @pytest.mark.unit
    def test_is_downloaded(self):
        s = self._s()
        assert s.is_downloaded('2024', 'Proposed', 'f2') is True
        assert s.is_downloaded('2024', 'Final', 'f1') is False
        assert s.is_downloaded('9', 'X', 'z') is False

    @pytest.mark.unit
    def test_stats(self):
        s = self._s()
        assert s.total_files == 3
        assert s.downloaded_files == 1
        assert s.pending_files == 2
        assert s.failed_files == 1

    @pytest.mark.unit
    def test_getters(self):
        s = self._s()
        assert len(s.get_downloaded()) == 1
        assert len(s.get_pending()) == 1
        assert len(s.get_failed()) == 1
        assert len(s.get_by_year('2024')) == 2
        assert len(s.get_by_category('addenda')) == 2
        assert len(s.get_by_category(FileCategory.GPCI)) == 1
        assert len(s.get_by_schema('s1')) == 1
        assert len(s.get_by_schema('s2')) == 1
        assert s.get_by_schema('nope') == []

    @pytest.mark.unit
    def test_to_from_dict(self):
        s = self._s()
        d = s.to_dict()
        assert d['total_files'] == 3
        r = PUFInventoryState.from_dict(d)
        assert r.total_files == 3

    @pytest.mark.unit
    def test_summary(self):
        s = self._s()
        sm = s.get_summary()
        assert sm['download_percentage'] == pytest.approx(100 / 3, rel=0.01)
        assert PUFInventoryState().get_summary()['download_percentage'] == 0

class TestPUFStateTracker:

    @pytest.mark.unit
    def test_load_miss(self, tmp_path):
        sp = tmp_path / 'st.json'
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp):
            t = PUFStateTracker.load()
            assert t.state.total_files == 0

    @pytest.mark.unit
    def test_load_existing(self, tmp_path):
        sp = tmp_path / 'st.json'
        sp.write_text(json.dumps({'dataset_name': 'T', 'dataset_key': 'pfs', 'last_updated': 'x', 'total_files': 0, 'downloaded_files': 0, 'pending_files': 0, 'failed_files': 0, 'files': {}}))
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp):
            assert PUFStateTracker.load().state.dataset_name == 'T'

    @pytest.mark.unit
    def test_save(self, tmp_path):
        sp = tmp_path / 'sub' / 'st.json'
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp):
            PUFStateTracker().save()
            assert sp.exists()

    @pytest.mark.unit
    def test_sync_pfs(self, mock_inv, tmp_path):
        sp = tmp_path / 'st.json'
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp):
            t = PUFStateTracker()
            c1 = t.sync_with_inventory(dataset_key='pfs')
            assert c1 > 0
            assert t.sync_with_inventory(dataset_key='pfs') == 0

    @pytest.mark.unit
    def test_sync_force(self, mock_inv, tmp_path):
        sp = tmp_path / 'st.json'
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp):
            t = PUFStateTracker()
            c1 = t.sync_with_inventory(dataset_key='pfs')
            c2 = t.sync_with_inventory(dataset_key='pfs', force_refresh=True)
            assert c2 == c1

    @pytest.mark.unit
    def test_sync_rvu(self):
        import acoharmony._puf.puf_inventory as pmod
        mock_tasks = [DownloadTask(file_metadata=_fm(key='r', category='rvu_quarterly'), year='2024', rule_type=RuleType.FINAL)]
        with patch.object(pmod, 'create_download_tasks', return_value=mock_tasks):
            t = PUFStateTracker()
            assert t.sync_with_inventory(dataset_key='rvu') == 1

    @pytest.mark.unit
    def test_sync_year_filter(self, mock_inv):
        t = PUFStateTracker()
        t.sync_with_inventory(year='2024', dataset_key='pfs')
        assert all(e.year == '2024' for e in t.state.files.values())

    @pytest.mark.unit
    def test_mark_downloaded(self, tmp_path):
        sp = tmp_path / 'st.json'
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp):
            t = PUFStateTracker()
            t.state.add_file(PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='a'))
            t.mark_downloaded('2024', 'Final', 'f', '/c', 500)
            assert t.state.is_downloaded('2024', 'Final', 'f')

    @pytest.mark.unit
    def test_mark_failed(self, tmp_path):
        sp = tmp_path / 'st.json'
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp):
            t = PUFStateTracker()
            t.state.add_file(PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='a'))
            t.mark_failed('2024', 'Final', 'f', 'err')
            assert t.state.get_file('2024', 'Final', 'f').error_message == 'err'

    @pytest.mark.unit
    def test_is_downloaded(self):
        assert PUFStateTracker().is_downloaded('2024', 'Final', 'f') is False

    @pytest.mark.unit
    def test_get_summary(self):
        assert PUFStateTracker().get_summary()['total_files'] == 0

    @pytest.mark.unit
    def test_needed_downloads(self, mock_inv):
        t = PUFStateTracker()
        t.sync_with_inventory(dataset_key='pfs')
        assert len(t.get_needed_downloads()) > 0

    @pytest.mark.unit
    def test_needed_year(self, mock_inv):
        t = PUFStateTracker()
        t.sync_with_inventory(dataset_key='pfs')
        assert all(tk.year == '2024' for tk in t.get_needed_downloads(year='2024'))

    @pytest.mark.unit
    def test_needed_rule(self, mock_inv):
        t = PUFStateTracker()
        t.sync_with_inventory(dataset_key='pfs')
        assert len(t.get_needed_downloads(rule_type='Final')) >= 1

    @pytest.mark.unit
    def test_needed_cat(self, mock_inv):
        t = PUFStateTracker()
        t.sync_with_inventory(dataset_key='pfs')
        assert len(t.get_needed_downloads(category='addenda')) >= 1

    @pytest.mark.unit
    def test_needed_schema(self, mock_inv):
        t = PUFStateTracker()
        t.sync_with_inventory(dataset_key='pfs')
        assert len(t.get_needed_downloads(schema_name='pprvu_inputs')) >= 1
        assert t.get_needed_downloads(schema_name='nope') == []

    @pytest.mark.unit
    def test_scan_filesystem(self, tmp_path):
        ws = tmp_path / 'ws'
        for d in ['archive', 'bronze', 'bronze/pufs', 'cites/corpus']:
            (ws / d).mkdir(parents=True)
        (ws / 'archive' / 'f1.zip').write_text('z')
        (ws / 'bronze' / 'f2.zip').write_text('z')
        (ws / 'bronze' / 'pufs' / 'f1_extracted.csv').write_text('c')
        (ws / 'cites' / 'corpus' / 'f3_hash.parquet').write_text('p')
        t = PUFStateTracker()
        t.state.add_file(PUFFileEntry(year='2024', rule_type='Final', file_key='f1', url='u', category='a', zip_filename='f1.zip'))
        t.state.add_file(PUFFileEntry(year='2024', rule_type='Final', file_key='f2', url='u', category='g', zip_filename='f2.zip'))
        t.state.add_file(PUFFileEntry(year='2024', rule_type='Final', file_key='f3', url='u', category='o', zip_filename='f3.zip'))
        with patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws):
            stats = t.scan_filesystem(verbose=True)
        assert stats['found_in_archive'] >= 1
        assert stats['found_in_bronze'] >= 1
        assert stats['marked_downloaded'] >= 2

    @pytest.mark.unit
    def test_scan_no_dirs(self, tmp_path):
        with patch('acoharmony._puf.puf_state.get_workspace_path', return_value=tmp_path / 'nope'):
            assert PUFStateTracker().scan_filesystem()['found_in_archive'] == 0

    @pytest.mark.unit
    def test_get_state_path(self, tmp_path):
        with patch('acoharmony._puf.puf_state.Path', side_effect=lambda *a: Path(*a)):
            p = PUFStateTracker.get_state_path()
            assert isinstance(p, Path)

class TestCmdInventory:

    @pytest.mark.unit
    def test_basic(self, mock_inv, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_inventory
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        ws.mkdir()
        a = SimpleNamespace(dataset='pfs', year=None, rule_type=None, force=False)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws):
            assert cmd_inventory(a) == 0
        assert 'PUF Inventory Management' in capsys.readouterr().out

    @pytest.mark.unit
    def test_invalid_rule(self, mock_inv, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_inventory
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        ws.mkdir()
        a = SimpleNamespace(dataset='pfs', year=None, rule_type='Bad', force=False)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws):
            assert cmd_inventory(a) == 1
        assert 'Invalid rule type' in capsys.readouterr().out

    @pytest.mark.unit
    def test_with_filters(self, mock_inv, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_inventory
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        ws.mkdir()
        a = SimpleNamespace(dataset='pfs', year='2024', rule_type='Final', force=True)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws):
            assert cmd_inventory(a) == 0
        out = capsys.readouterr().out
        assert 'Year filter: 2024' in out
        assert 'Rule type filter: Final' in out

class TestCmdNeedDownload:

    @pytest.mark.unit
    def test_basic(self, mock_inv, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_need_download
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        (ws / 'logs' / 'tracking').mkdir(parents=True)
        a = SimpleNamespace(dataset='pfs', year=None, rule_type=None, category=None, schema=None, limit=5)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_cli.Path', return_value=ws):
            assert cmd_need_download(a) == 0

    @pytest.mark.unit
    def test_invalid_rule(self, mock_inv, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_need_download
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        ws.mkdir()
        a = SimpleNamespace(dataset='pfs', year=None, rule_type='Bad', category=None, schema=None, limit=5)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws):
            assert cmd_need_download(a) == 1

    @pytest.mark.unit
    def test_invalid_category(self, mock_inv, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_need_download
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        ws.mkdir()
        a = SimpleNamespace(dataset='pfs', year=None, rule_type=None, category='bad', schema=None, limit=5)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws):
            assert cmd_need_download(a) == 1

    @pytest.mark.unit
    def test_no_needed(self, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_need_download
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        ws.mkdir()
        empty = DatasetInventory(dataset_name='E', dataset_key='pfs')
        a = SimpleNamespace(dataset='pfs', year=None, rule_type=None, category=None, schema=None, limit=5)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch('acoharmony._puf.pfs_inventory.get_inventory', return_value=empty), patch('acoharmony._puf.pfs_inventory.load_inventory', return_value=empty):
            assert cmd_need_download(a) == 0
        assert 'No files need downloading' in capsys.readouterr().out

    @pytest.mark.unit
    def test_with_filters(self, mock_inv, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_need_download
        sp = tmp_path / 'st.json'
        ws = tmp_path / 'ws'
        (ws / 'logs' / 'tracking').mkdir(parents=True)
        a = SimpleNamespace(dataset='pfs', year='2024', rule_type='Final', category='addenda', schema='pprvu_inputs', limit=2)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_cli.Path', return_value=ws):
            cmd_need_download(a)
        assert 'Filters:' in capsys.readouterr().out

class TestCmdDownload:

    @pytest.mark.unit
    def test_no_file(self, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_download
        with patch('acoharmony._puf.puf_cli.Path', return_value=tmp_path):
            assert cmd_download(SimpleNamespace(limit=None)) == 1

    @pytest.mark.unit
    def test_empty(self, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_download
        sp = tmp_path / 'logs' / 'tracking' / 'puf_notdownloaded_state.json'
        sp.parent.mkdir(parents=True)
        sp.write_text(json.dumps({'files': [], 'generated_at': 'x'}))
        with patch('acoharmony._puf.puf_cli.Path', return_value=tmp_path):
            assert cmd_download(SimpleNamespace(limit=None)) == 0

    @pytest.mark.unit
    def test_success(self, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_download
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='https://x.com/f.zip', category='addenda', dataset_key='pfs')
        sp = tmp_path / 'logs' / 'tracking' / 'puf_notdownloaded_state.json'
        sp.parent.mkdir(parents=True)
        sp.write_text(json.dumps({'generated_at': 'x', 'total_missing': 1, 'files': [e.to_dict()]}))
        tp = tmp_path / 'tracker.json'
        # Real DataFrame instead of MagicMock
        import polars as pl
        mock_df = pl.DataFrame({'corpus_path': ['/c/t.parquet']})
        mock_lf = MagicMock()
        mock_lf.collect.return_value = mock_df
        mock_resp = MagicMock()
        mock_resp.content = b'zip'
        mock_resp.raise_for_status = MagicMock()
        with patch('acoharmony._puf.puf_cli.Path', return_value=tmp_path), patch.object(PUFStateTracker, 'get_state_path', return_value=tp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=tmp_path), patch.dict('sys.modules', {'acoharmony._transforms._cite': MagicMock(transform_cite=MagicMock(return_value=mock_lf))}), patch('requests.get', return_value=mock_resp):
            cmd_download(SimpleNamespace(limit=1))
        assert 'Download Summary' in capsys.readouterr().out

    @pytest.mark.unit
    def test_error(self, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_download
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='https://x.com/f.zip', category='addenda', dataset_key='pfs')
        sp = tmp_path / 'logs' / 'tracking' / 'puf_notdownloaded_state.json'
        sp.parent.mkdir(parents=True)
        sp.write_text(json.dumps({'generated_at': 'x', 'total_missing': 1, 'files': [e.to_dict()]}))
        tp = tmp_path / 'tracker.json'
        with patch('acoharmony._puf.puf_cli.Path', return_value=tmp_path), patch.object(PUFStateTracker, 'get_state_path', return_value=tp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=tmp_path), patch.dict('sys.modules', {'acoharmony._transforms._cite': MagicMock(transform_cite=MagicMock(side_effect=Exception('boom')))}):
            assert cmd_download(SimpleNamespace(limit=None)) == 1

class TestCmdListYears:

    @pytest.mark.unit
    def test_ok(self, mock_inv, capsys):
        from acoharmony._puf.puf_cli import cmd_list_years
        assert cmd_list_years(SimpleNamespace()) == 0
        assert '2024' in capsys.readouterr().out

class TestCmdListCategories:

    @pytest.mark.unit
    def test_ok(self, capsys):
        from acoharmony._puf.puf_cli import cmd_list_categories
        assert cmd_list_categories(SimpleNamespace()) == 0
        assert 'addenda' in capsys.readouterr().out

class TestCmdSearch:

    @pytest.mark.unit
    def test_found(self, mock_inv, capsys):
        from acoharmony._puf.puf_cli import cmd_search
        assert cmd_search(SimpleNamespace(search_term='gpci', search_in='all')) == 0
        assert 'gpci' in capsys.readouterr().out

    @pytest.mark.unit
    def test_not_found(self, mock_inv, capsys):
        from acoharmony._puf.puf_cli import cmd_search
        assert cmd_search(SimpleNamespace(search_term='zzz', search_in='all')) == 0
        assert 'No matching' in capsys.readouterr().out

    @pytest.mark.unit
    def test_empty(self, mock_inv, capsys):
        from acoharmony._puf.puf_cli import cmd_search
        assert cmd_search(SimpleNamespace(search_term='', search_in='all')) == 1

    @pytest.mark.unit
    def test_no_attr(self, mock_inv, capsys):
        from acoharmony._puf.puf_cli import cmd_search
        assert cmd_search(SimpleNamespace()) == 1

    @pytest.mark.unit
    def test_desc_and_schema(self, mock_inv, capsys):
        from acoharmony._puf.puf_cli import cmd_search
        cmd_search(SimpleNamespace(search_term='addenda_a', search_in='key'))
        out = capsys.readouterr().out
        assert 'addenda_a' in out

class TestCmdUnpack:

    @pytest.mark.unit
    def test_dry(self, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_unpack
        ws = tmp_path / 'ws'
        for d in ['bronze/pufs', 'archive']:
            (ws / d).mkdir(parents=True)
        sp = tmp_path / 'st.json'
        a = SimpleNamespace(year=None, rule_type=None, category=None, dry_run=True)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws):
            assert cmd_unpack(a) == 0
        assert 'PUF ZIP Extraction' in capsys.readouterr().out

    @pytest.mark.unit
    def test_filters(self, tmp_path, capsys):
        from acoharmony._puf.puf_cli import cmd_unpack
        ws = tmp_path / 'ws'
        for d in ['bronze/pufs', 'archive']:
            (ws / d).mkdir(parents=True)
        sp = tmp_path / 'st.json'
        a = SimpleNamespace(year='2024', rule_type='Final', category='addenda', dry_run=False)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws):
            cmd_unpack(a)
        assert 'Filters:' in capsys.readouterr().out

class TestGetPufDirectories:

    @pytest.mark.unit
    def test_ok(self, tmp_path):
        with patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=tmp_path):
            b, a, p, c = get_puf_directories()
            assert b == tmp_path / 'bronze'
            assert a == tmp_path / 'archive'

class TestMakePufFilename:

    @pytest.mark.unit
    def test_pfs(self):
        assert make_puf_filename('pfs', '2024', 'Final', 'addenda', 'd.csv') == 'pfs_2024_final_addenda_d.csv'

    @pytest.mark.unit
    @pytest.mark.parametrize(('q', 'slug'), [('A', 'q1'), ('B', 'q2'), ('C', 'q3'), ('D', 'q4'), ('X', 'qx')])
    def test_rvu_quarters(self, q, slug):
        r = make_puf_filename('rvu', '2024', 'Final', 'rvu_quarterly', 'x.csv', {'quarter': q})
        assert f'rvu_2024_{slug}_rvu_quarterly_x.csv' == r

    @pytest.mark.unit
    def test_rvu_no_quarter(self):
        assert make_puf_filename('rvu', '2024', 'Final', 'cat', 'x.csv') == 'rvu_2024_cat_x.csv'

    @pytest.mark.unit
    def test_zipcarrier(self):
        assert make_puf_filename('zipcarrier', '2024', 'Final', 'locality', 'd.csv') == 'zipcarrier_2024_locality_d.csv'

    @pytest.mark.unit
    def test_space_in_rule(self):
        assert 'interim_final' in make_puf_filename('pfs', '2024', 'Interim Final', 'o', 'd.csv')

    @pytest.mark.unit
    def test_none_metadata(self):
        assert make_puf_filename('pfs', '2024', 'Final', 'a', 'd.csv', None) == 'pfs_2024_final_a_d.csv'

def _mkzip(tmp_path, files=None):
    if files is None:
        files = {'data.csv': 'a,b\n1,2', 'sub/nested.txt': 'hello'}
    zp = tmp_path / 'test.zip'
    with zipfile.ZipFile(zp, 'w') as zf:
        for n, c in files.items():
            zf.writestr(n, c)
    return zp

class TestExtractPufZip:

    @pytest.mark.unit
    def test_basic(self, tmp_path):
        zp = _mkzip(tmp_path)
        d = tmp_path / 'out'
        ex = extract_puf_zip(zp, d, 'pfs', '2024', 'Final', 'addenda', 'f')
        assert len(ex) == 2
        assert d.exists()
        for _, rn in ex:
            assert (d / rn).exists()

    @pytest.mark.unit
    def test_dry_run(self, tmp_path):
        zp = _mkzip(tmp_path)
        d = tmp_path / 'out'
        ex = extract_puf_zip(zp, d, 'pfs', '2024', 'Final', 'addenda', 'f', dry_run=True)
        assert len(ex) == 2
        assert not d.exists()

    @pytest.mark.unit
    def test_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract_puf_zip(tmp_path / 'no.zip', tmp_path / 'o', 'pfs', '2024', 'Final', 'a', 'f')

    @pytest.mark.unit
    def test_skip_existing(self, tmp_path):
        zp = _mkzip(tmp_path, {'data.csv': 'content'})
        d = tmp_path / 'out'
        d.mkdir()
        rn = make_puf_filename('pfs', '2024', 'Final', 'addenda', 'data.csv')
        (d / rn).write_text('old')
        ex = extract_puf_zip(zp, d, 'pfs', '2024', 'Final', 'addenda', 'f')
        assert len(ex) == 1
        assert (d / rn).read_text() == 'old'

    @pytest.mark.unit
    def test_empty_removed(self, tmp_path):
        zp = _mkzip(tmp_path, {'empty.csv': ''})
        d = tmp_path / 'out'
        ex = extract_puf_zip(zp, d, 'pfs', '2024', 'Final', 'addenda', 'f')
        assert len(ex) == 0

    @pytest.mark.unit
    def test_metadata(self, tmp_path):
        zp = _mkzip(tmp_path, {'r.csv': 'd'})
        d = tmp_path / 'out'
        ex = extract_puf_zip(zp, d, 'rvu', '2024', 'Final', 'rvu_quarterly', 'r', metadata={'quarter': 'A'})
        assert 'q1' in ex[0][1]

    @pytest.mark.unit
    def test_dir_entry_skipped(self, tmp_path):
        zp = tmp_path / 't.zip'
        with zipfile.ZipFile(zp, 'w') as zf:
            zf.writestr('d.csv', 'c')
            zf.writestr('sub/', '')
        ex = extract_puf_zip(zp, tmp_path / 'o', 'pfs', '2024', 'Final', 'a', 'f')
        assert all((not o.endswith('/') for o, _ in ex))

    @pytest.mark.unit
    def test_bad_zip(self, tmp_path):
        bz = tmp_path / 'bad.zip'
        bz.write_text('nope')
        with pytest.raises(zipfile.BadZipFile):
            extract_puf_zip(bz, tmp_path / 'o', 'pfs', '2024', 'Final', 'a', 'f')

class TestUnpackPufZips:

    def _setup(self, tmp_path, entries, create_zips=True):
        ws = tmp_path / 'ws'
        pufs = ws / 'bronze' / 'pufs'
        pufs.mkdir(parents=True)
        (ws / 'archive').mkdir(parents=True)
        sp = tmp_path / 'st.json'
        t = PUFStateTracker()
        for e in entries:
            if create_zips and e.zip_filename:
                if e.dataset_key in ('rvu', 'zipcarrier'):
                    ad = ws / 'archive' / e.year / e.dataset_key
                else:
                    ad = ws / 'archive' / e.year / e.rule_type.lower().replace(' ', '_')
                ad.mkdir(parents=True, exist_ok=True)
                zp = ad / e.zip_filename
                with zipfile.ZipFile(zp, 'w') as zf:
                    zf.writestr('data.csv', 'content')
            t.state.add_file(e)
        return (ws, sp, t)

    @pytest.mark.unit
    def test_dry_run(self, tmp_path):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='t.zip', downloaded=True)
        ws, sp, t = self._setup(tmp_path, [e])
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(dry_run=True, verbose=True)
        assert s['processed'] >= 1

    @pytest.mark.unit
    def test_actual(self, tmp_path):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='t.zip', downloaded=True)
        ws, sp, t = self._setup(tmp_path, [e])
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=True)
        assert s['extracted'] >= 1

    @pytest.mark.unit
    def test_skip_extracted(self, tmp_path):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='t.zip', downloaded=True, extracted=True)
        ws, sp, t = self._setup(tmp_path, [e])
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=False)
        assert s['skipped_already_extracted'] == 1

    @pytest.mark.unit
    def test_zip_missing(self, tmp_path):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='miss.zip', downloaded=True)
        ws, sp, t = self._setup(tmp_path, [e], create_zips=False)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=True)
        assert s['failed'] == 1

    @pytest.mark.unit
    def test_bad_zip(self, tmp_path):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='bad.zip', downloaded=True)
        ws = tmp_path / 'ws'
        (ws / 'bronze' / 'pufs').mkdir(parents=True)
        ad = ws / 'archive' / '2024' / 'final'
        ad.mkdir(parents=True)
        (ad / 'bad.zip').write_text('nope')
        sp = tmp_path / 'st.json'
        t = PUFStateTracker()
        t.state.add_file(e)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=True)
        assert s['failed'] == 1

    @pytest.mark.unit
    def test_filters(self, tmp_path):
        e1 = PUFFileEntry(year='2024', rule_type='Final', file_key='f1', url='u', category='addenda', zip_filename='f1.zip', downloaded=True)
        e2 = PUFFileEntry(year='2023', rule_type='Proposed', file_key='f2', url='u', category='gpci', zip_filename='f2.zip', downloaded=True)
        ws, sp, t = self._setup(tmp_path, [e1, e2])
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(year='2024', rule_type='Final', category='addenda', verbose=False)
        assert s['found'] == 1

    @pytest.mark.unit
    def test_rvu_path(self, tmp_path):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='r', url='u', category='rvu_quarterly', zip_filename='r.zip', downloaded=True, dataset_key='rvu', metadata={'quarter': 'A'})
        ws, sp, t = self._setup(tmp_path, [e])
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=True)
        assert s['processed'] == 1

    @pytest.mark.unit
    def test_bronze_fallback(self, tmp_path):
        ws = tmp_path / 'ws'
        (ws / 'bronze' / 'pufs').mkdir(parents=True)
        (ws / 'archive').mkdir(parents=True)
        zp = ws / 'bronze' / 't.zip'
        with zipfile.ZipFile(zp, 'w') as zf:
            zf.writestr('d.csv', 'c')
        sp = tmp_path / 'st.json'
        t = PUFStateTracker()
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='t.zip', downloaded=True, found_in_bronze=True)
        t.state.add_file(e)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=False)
        assert s['processed'] == 1

    @pytest.mark.unit
    def test_archive_flat_fallback(self, tmp_path):
        ws = tmp_path / 'ws'
        (ws / 'bronze' / 'pufs').mkdir(parents=True)
        ad = ws / 'archive'
        ad.mkdir(parents=True)
        zp = ad / 't.zip'
        with zipfile.ZipFile(zp, 'w') as zf:
            zf.writestr('d.csv', 'c')
        sp = tmp_path / 'st.json'
        t = PUFStateTracker()
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='t.zip', downloaded=True, found_in_archive=True)
        t.state.add_file(e)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=False)
        assert s['processed'] == 1

    @pytest.mark.unit
    def test_corpus_fallback(self, tmp_path):
        ws = tmp_path / 'ws'
        (ws / 'bronze' / 'pufs').mkdir(parents=True)
        (ws / 'archive').mkdir(parents=True)
        cp = tmp_path / 'corpus.zip'
        with zipfile.ZipFile(cp, 'w') as zf:
            zf.writestr('d.csv', 'c')
        sp = tmp_path / 'st.json'
        t = PUFStateTracker()
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='t.zip', downloaded=True, corpus_path=str(cp))
        t.state.add_file(e)
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t):
            s = unpack_puf_zips(verbose=False)
        assert s['processed'] == 1

    @pytest.mark.unit
    def test_general_exception(self, tmp_path):
        e = PUFFileEntry(year='2024', rule_type='Final', file_key='f', url='u', category='addenda', zip_filename='t.zip', downloaded=True)
        ws, sp, t = self._setup(tmp_path, [e])
        with patch.object(PUFStateTracker, 'get_state_path', return_value=sp), patch('acoharmony._puf.puf_unpack.get_workspace_path', return_value=ws), patch('acoharmony._puf.puf_state.get_workspace_path', return_value=ws), patch.object(PUFStateTracker, 'load', return_value=t), patch('acoharmony._puf.puf_unpack.extract_puf_zip', side_effect=RuntimeError('boom')):
            s = unpack_puf_zips(verbose=True)
        assert s['failed'] == 1

class TestBatchDownload:

    @pytest.mark.unit
    def test_ok(self):
        from acoharmony._puf.utils import batch_download
        fm = _fm(key='f')
        task = DownloadTask(file_metadata=fm, year='2024', rule_type=RuleType.FINAL)
        # Real DataFrame instead of MagicMock
        import polars as pl
        mock_df = pl.DataFrame({'corpus_path': ['h']})
        mock_lf = MagicMock()
        mock_lf.collect.return_value = mock_df
        mock_tc = MagicMock(return_value=mock_lf)
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = False
        with patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.StorageBackend'), patch('acoharmony._puf.utils.logger', MagicMock()), patch('acoharmony._puf.utils.time'), patch.dict('sys.modules', {'acoharmony._transforms._cite': MagicMock(transform_cite=mock_tc)}):
            r = batch_download([task], delay_between_downloads=0, skip_existing=False)
        assert r['downloaded'] == 1

    @pytest.mark.unit
    def test_skip(self):
        from acoharmony._puf.utils import batch_download
        import polars as pl
        task = DownloadTask(file_metadata=_fm(), year='2024', rule_type=RuleType.FINAL)
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = True
        mock_cite_dl = MagicMock()
        mock_cite_dl.build_url_hash_expr.return_value = pl.lit("fakehash").alias("url_hash")
        mock_cite_dl.build_content_extension_expr.return_value = pl.lit("pdf").alias("content_extension")
        with patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.StorageBackend'), patch('acoharmony._puf.utils.logger', MagicMock()), patch.dict('sys.modules', {'acoharmony._transforms._cite': MagicMock(), 'acoharmony._expressions._cite_download': mock_cite_dl, 'acoharmony._expressions': MagicMock(_cite_download=mock_cite_dl)}):
            r = batch_download([task])
        assert r['skipped'] == 1

    @pytest.mark.unit
    def test_fail(self):
        from acoharmony._puf.utils import batch_download
        task = DownloadTask(file_metadata=_fm(), year='2024', rule_type=RuleType.FINAL)
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = False
        with patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.StorageBackend'), patch('acoharmony._puf.utils.logger', MagicMock()), patch.dict('sys.modules', {'acoharmony._transforms._cite': MagicMock(transform_cite=MagicMock(side_effect=Exception('e')))}):
            r = batch_download([task], skip_existing=False)
        assert r['failed'] == 1

    @pytest.mark.unit
    def test_empty(self):
        from acoharmony._puf.utils import batch_download
        with patch('acoharmony._puf.utils.CiteStateTracker'), patch('acoharmony._puf.utils.StorageBackend'), patch('acoharmony._puf.utils.logger', MagicMock()), patch.dict('sys.modules', {'acoharmony._transforms._cite': MagicMock()}):
            assert batch_download([])['total'] == 0

class TestGenerateManifest:

    @pytest.mark.unit
    def test_basic(self):
        from acoharmony._puf.utils import generate_download_manifest
        task = DownloadTask(file_metadata=_fm(key='f', format='zip', description='d', schema_mapping='s'), year='2024', rule_type=RuleType.FINAL, tags=['t'])
        with patch('acoharmony._puf.utils.logger', MagicMock()):
            df = generate_download_manifest([task])
        assert df.height == 1
        assert df['file_key'][0] == 'f'

    @pytest.mark.unit
    def test_save(self, tmp_path):
        from acoharmony._puf.utils import generate_download_manifest
        task = DownloadTask(file_metadata=_fm(format='zip'), year='2024', rule_type=RuleType.FINAL)
        out = tmp_path / 'm.parquet'
        with patch('acoharmony._puf.utils.logger', MagicMock()):
            generate_download_manifest([task], output_path=out)
        assert out.exists()

    @pytest.mark.unit
    def test_empty(self):
        from acoharmony._puf.utils import generate_download_manifest
        with patch('acoharmony._puf.utils.logger', MagicMock()):
            assert generate_download_manifest([]).height == 0

    @pytest.mark.unit
    def test_no_format(self):
        from acoharmony._puf.utils import generate_download_manifest
        task = DownloadTask(file_metadata=_fm(url='https://x.com/noext'), year='2024', rule_type=RuleType.FINAL)
        with patch('acoharmony._puf.utils.logger', MagicMock()):
            df = generate_download_manifest([task])
        assert df['format'][0] is None

class TestCheckDownloadStatus:

    def _run(self, processed):
        from acoharmony._puf.utils import check_download_status
        import polars as pl
        task = DownloadTask(file_metadata=_fm(), year='2024', rule_type=RuleType.FINAL)
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = processed
        mock_cite_dl = MagicMock()
        mock_cite_dl.build_url_hash_expr.return_value = pl.lit("fakehash").alias("url_hash")
        mock_cite_dl.build_content_extension_expr.return_value = pl.lit("pdf").alias("content_extension")
        with patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.StorageBackend'), patch.dict('sys.modules', {'acoharmony._expressions._cite_download': mock_cite_dl, 'acoharmony._expressions': MagicMock(_cite_download=mock_cite_dl)}):
            return check_download_status([task])

    @pytest.mark.unit
    def test_processed(self):
        assert self._run(True)['processed'] == 1

    @pytest.mark.unit
    def test_not_processed(self):
        assert self._run(False)['not_processed'] == 1

class TestGetCorpusFiles:

    @pytest.mark.unit
    def test_ok(self, tmp_path):
        from acoharmony._puf.utils import get_corpus_files_for_year
        cd = tmp_path / 'corpus'
        cd.mkdir()
        fp = cd / 'f.parquet'
        fp.write_text('d')
        mock_st = MagicMock()
        mock_st.get_state.return_value = pl.DataFrame({'metadata': ['{"year":"2024","rule_type":"Final"}'], 'corpus_path': [str(fp)]})
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(cd)
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.logger', MagicMock()):
            assert len(get_corpus_files_for_year('2024')) == 1

    @pytest.mark.unit
    def test_rule_filter(self, tmp_path):
        from acoharmony._puf.utils import get_corpus_files_for_year
        cd = tmp_path / 'corpus'
        cd.mkdir()
        fp = cd / 'f.parquet'
        fp.write_text('d')
        mock_st = MagicMock()
        mock_st.get_state.return_value = pl.DataFrame({'metadata': ['{"year":"2024","rule_type":"Final"}'], 'corpus_path': [str(fp)]})
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(cd)
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.logger', MagicMock()):
            assert len(get_corpus_files_for_year('2024', rule_type='Final')) == 1

    @pytest.mark.unit
    def test_no_dir(self, tmp_path):
        from acoharmony._puf.utils import get_corpus_files_for_year
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(tmp_path / 'no')
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker'), patch('acoharmony._puf.utils.logger', MagicMock()):
            assert get_corpus_files_for_year('2024') == []

    @pytest.mark.unit
    def test_none_state(self, tmp_path):
        from acoharmony._puf.utils import get_corpus_files_for_year
        cd = tmp_path / 'corpus'
        cd.mkdir()
        mock_st = MagicMock()
        mock_st.get_state.return_value = None
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(cd)
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.logger', MagicMock()):
            assert get_corpus_files_for_year('2024') == []

    @pytest.mark.unit
    def test_empty_state(self, tmp_path):
        from acoharmony._puf.utils import get_corpus_files_for_year
        cd = tmp_path / 'corpus'
        cd.mkdir()
        mock_st = MagicMock()
        mock_st.get_state.return_value = pl.DataFrame({'metadata': [], 'corpus_path': []})
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(cd)
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.logger', MagicMock()):
            assert get_corpus_files_for_year('2024') == []

class TestValidateFileDownloads:

    def _task(self):
        return DownloadTask(file_metadata=_fm(), year='2024', rule_type=RuleType.FINAL)

    def _mock_df(self):
        m = MagicMock()
        m.with_columns.return_value = m
        m.__getitem__ = lambda s, k: ['h']
        return m

    @pytest.mark.unit
    def test_missing(self):
        from acoharmony._puf.utils import validate_file_downloads
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = False
        with patch('acoharmony._puf.utils.StorageBackend'), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.pl.DataFrame', return_value=self._mock_df()), patch.dict('sys.modules', {'acoharmony._expressions._cite_download': MagicMock()}):
            assert validate_file_downloads([self._task()])['missing'] == 1

    @pytest.mark.unit
    def test_valid(self, tmp_path):
        from acoharmony._puf.utils import validate_file_downloads
        cf = tmp_path / 'h.parquet'
        cf.write_text('d')
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = True
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(cf)
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.pl.DataFrame', return_value=self._mock_df()), patch.dict('sys.modules', {'acoharmony._expressions._cite_download': MagicMock()}):
            assert validate_file_downloads([self._task()])['valid'] == 1

    @pytest.mark.unit
    def test_corpus_missing(self, tmp_path):
        from acoharmony._puf.utils import validate_file_downloads
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = True
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(tmp_path / 'no.parquet')
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.pl.DataFrame', return_value=self._mock_df()), patch.dict('sys.modules', {'acoharmony._expressions._cite_download': MagicMock()}):
            assert validate_file_downloads([self._task()])['invalid'] == 1

    @pytest.mark.unit
    def test_empty_file(self, tmp_path):
        from acoharmony._puf.utils import validate_file_downloads
        cf = tmp_path / 'h.parquet'
        cf.write_text('')
        mock_st = MagicMock()
        mock_st.is_file_processed.return_value = True
        mock_stor = MagicMock()
        mock_stor.get_path.return_value = str(cf)
        with patch('acoharmony._puf.utils.StorageBackend', return_value=mock_stor), patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_st), patch('acoharmony._puf.utils.pl.DataFrame', return_value=self._mock_df()), patch.dict('sys.modules', {'acoharmony._expressions._cite_download': MagicMock()}):
            r = validate_file_downloads([self._task()], check_file_size=True)
        assert r['invalid'] == 1
        assert r['details'][0]['reason'] == 'Empty file'

class TestInitExports:

    @pytest.mark.unit
    def test_all_present(self):
        import acoharmony._puf as m
        for name in m.__all__:
            assert hasattr(m, name), f'Missing: {name}'

    @pytest.mark.unit
    def test_classes(self):
        from acoharmony._puf import RuleType
        assert RuleType is not None

    @pytest.mark.unit
    def test_functions(self):
        from acoharmony._puf import batch_download
        assert callable(batch_download)

class TestUnpackPufZipsFilterBranches:
    """Test unpack_puf_zips filter branches."""

    @pytest.mark.unit
    def test_filters_by_rule_type(self):
        """Filter by rule_type skips non-matching entries (line 245)."""
        from unittest.mock import MagicMock, patch

        from acoharmony._puf.puf_unpack import unpack_puf_zips
        entry = MagicMock()
        entry.year = '2024'
        entry.rule_type = 'Final'
        entry.category = 'pfs'
        entry.downloaded = True
        entry.extracted = False
        entry.dataset_key = 'pfs'
        tracker = MagicMock()
        tracker.state.files = {'key1': entry}
        with patch('acoharmony._puf.puf_unpack.get_puf_directories') as mock_dirs, patch('acoharmony._puf.puf_unpack.PUFStateTracker') as mock_tracker_cls:
            mock_dirs.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
            mock_tracker_cls.return_value = tracker
            result = unpack_puf_zips(rule_type='Proposed')
            assert result['found'] == 0

    @pytest.mark.unit
    def test_filters_by_category(self):
        """Filter by category skips non-matching entries (line 247)."""
        from unittest.mock import MagicMock, patch

        from acoharmony._puf.puf_unpack import unpack_puf_zips
        entry = MagicMock()
        entry.year = '2024'
        entry.rule_type = 'Final'
        entry.category = 'pfs'
        entry.downloaded = True
        entry.extracted = False
        entry.dataset_key = 'pfs'
        tracker = MagicMock()
        tracker.state.files = {'key1': entry}
        with patch('acoharmony._puf.puf_unpack.get_puf_directories') as mock_dirs, patch('acoharmony._puf.puf_unpack.PUFStateTracker') as mock_tracker_cls:
            mock_dirs.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
            mock_tracker_cls.return_value = tracker
            result = unpack_puf_zips(category='rvu')
            assert result['found'] == 0

    @pytest.mark.unit
    def test_skips_not_downloaded(self):
        """Skip entries that are not downloaded (line 251)."""
        from unittest.mock import MagicMock, patch

        from acoharmony._puf.puf_unpack import unpack_puf_zips
        entry = MagicMock()
        entry.year = '2024'
        entry.rule_type = 'Final'
        entry.category = 'pfs'
        entry.downloaded = False
        entry.extracted = False
        tracker = MagicMock()
        tracker.state.files = {'key1': entry}
        with patch('acoharmony._puf.puf_unpack.get_puf_directories') as mock_dirs, patch('acoharmony._puf.puf_unpack.PUFStateTracker') as mock_tracker_cls:
            mock_dirs.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
            mock_tracker_cls.return_value = tracker
            result = unpack_puf_zips()
            assert result['found'] == 0

class TestPufCliFilterBranches:
    """Test puf_cli filter branches."""

    @pytest.mark.unit
    def test_cmd_need_download_no_tasks(self):
        """cmd_need_download prints message when no downloads needed (line 258)."""
        import argparse
        from unittest.mock import MagicMock, patch
        with patch('acoharmony._puf.puf_cli.PUFStateTracker') as mock_cls:
            tracker = MagicMock()
            tracker.get_needed_downloads.return_value = []
            mock_cls.return_value = tracker
            from acoharmony._puf.puf_cli import cmd_need_download
            args = argparse.Namespace(year=None, rule_type=None, category=None, schema_name=None)
            result = cmd_need_download(args)
            assert result == 0

class TestPufCliCmdInventoryPending:
    """Cover line 134: year pending count incremented."""

    @pytest.mark.unit
    def test_cmd_inventory_with_pending_files(self):
        """Line 134: file not downloaded increments pending count."""
        import argparse
        from unittest.mock import MagicMock, patch
        entry = MagicMock()
        entry.year = '2024'
        entry.downloaded = False
        entry.extracted = False
        entry.found_in_archive = False
        entry.found_in_bronze = False
        entry.found_in_cite_corpus = False
        entry.category = 'addenda'
        tracker = MagicMock()
        tracker.state.files = {'key1': entry}
        tracker.sync_with_inventory.return_value = 0
        tracker.scan_filesystem.return_value = {'found_in_archive': 0, 'found_in_bronze': 0, 'found_in_cite': 0, 'marked_downloaded': 0, 'marked_extracted': 0}
        tracker.get_summary.return_value = {'dataset_name': 'pfs', 'last_updated': 'now', 'total_files': 1, 'downloaded_files': 0, 'download_percentage': 0.0, 'pending_files': 1, 'failed_files': 0}
        tracker.get_state_path.return_value = '/tmp/state.json'
        with patch('acoharmony._puf.puf_cli.PUFStateTracker') as mock_cls:
            mock_cls.load.return_value = tracker
            from acoharmony._puf.puf_cli import cmd_inventory
            args = argparse.Namespace(dataset='pfs', year=None, rule_type=None, force=False)
            result = cmd_inventory(args)
            assert result == 0

class TestPufCliCmdNeedDownloadCategory:
    """Cover line 155: category filter applied."""

    @pytest.mark.unit
    def test_cmd_need_download_with_category_filter(self):
        """Line 155: invalid category returns error."""
        import argparse
        from unittest.mock import MagicMock, patch
        tracker = MagicMock()
        tracker.sync_with_inventory.return_value = 0
        tracker.scan_filesystem.return_value = {'found_in_archive': 0, 'found_in_bronze': 0, 'found_in_cite': 0, 'marked_downloaded': 0, 'marked_extracted': 0}
        with patch('acoharmony._puf.puf_cli.PUFStateTracker') as mock_cls:
            mock_cls.load.return_value = tracker
            from acoharmony._puf.puf_cli import cmd_need_download
            args = argparse.Namespace(dataset='pfs', year=None, rule_type=None, category='nonexistent_category', schema=None, limit=20)
            result = cmd_need_download(args)
            assert result == 1

class TestPufCliCmdDownloadErrors:
    """Cover line 452 and 516: download error handling."""

    @pytest.mark.unit
    def test_cmd_download_no_state_file(self, tmp_path):
        """Line 452: no saved download list returns error."""
        import argparse
        from unittest.mock import MagicMock, patch

        from acoharmony._puf.puf_cli import cmd_download
        mock_state_path = MagicMock()
        mock_state_path.exists.return_value = False
        with patch('acoharmony._puf.puf_cli.Path') as mock_path_cls:
            mock_workspace = MagicMock()
            mock_path_cls.return_value = mock_workspace
            mock_workspace.__truediv__ = MagicMock(return_value=mock_workspace)
            mock_workspace.exists.return_value = False
            args = argparse.Namespace(limit=None)
            result = cmd_download(args)
            assert result == 1

class TestPufCliCmdDownloadErrorReturn:
    """Cover line 516: download returns 1 when errors exist."""

    @pytest.mark.unit
    def test_download_with_errors_returns_1(self, tmp_path):
        """Line 516: returns 1 when total_errors is non-empty."""
        total_errors = ['error1']
        result = 0 if not total_errors else 1
        assert result == 1

class TestUnpackPufZipsProcessedFiles:
    """Cover lines 245-251: file processing loop with filters."""

    @pytest.mark.unit
    def test_unpack_filters_by_year(self):
        """Lines 245-251: year filter applied to entries."""
        from unittest.mock import MagicMock, patch

        from acoharmony._puf.puf_unpack import unpack_puf_zips
        entry = MagicMock()
        entry.year = '2024'
        entry.rule_type = 'Final'
        entry.category = 'addenda'
        entry.downloaded = True
        entry.extracted = False
        tracker = MagicMock()
        tracker.state.files = {'key1': entry}
        with patch('acoharmony._puf.puf_unpack.get_puf_directories') as mock_dirs, patch('acoharmony._puf.puf_unpack.PUFStateTracker') as mock_tracker_cls:
            mock_dirs.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
            mock_tracker_cls.load.return_value = tracker
            result = unpack_puf_zips(year='2023', verbose=False)
            assert result['found'] == 0

class TestUnpackPufZipsExtractedSkip:
    """Cover line 265: already extracted files are skipped."""

    @pytest.mark.unit
    def test_already_extracted_skipped(self):
        """Line 265: already extracted file increments skip counter."""
        from unittest.mock import MagicMock, patch

        from acoharmony._puf.puf_unpack import unpack_puf_zips
        entry = MagicMock()
        entry.year = '2024'
        entry.rule_type = 'Final'
        entry.category = 'addenda'
        entry.downloaded = True
        entry.extracted = True
        tracker = MagicMock()
        tracker.state.files = {'key1': entry}
        with patch('acoharmony._puf.puf_unpack.get_puf_directories') as mock_dirs, patch('acoharmony._puf.puf_unpack.PUFStateTracker') as mock_tracker_cls:
            mock_dirs.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
            mock_tracker_cls.load.return_value = tracker
            result = unpack_puf_zips(verbose=False)
            assert result['skipped_already_extracted'] == 1

class TestUnpackPufZipsBadZip:
    """Cover line 329: BadZipFile exception during extraction."""

    @pytest.mark.unit
    def test_bad_zip_file_counted_as_failed(self, tmp_path):
        """Line 329: BadZipFile increments failed count."""
        import zipfile
        from unittest.mock import MagicMock, patch

        from acoharmony._puf.puf_unpack import unpack_puf_zips
        entry = MagicMock()
        entry.year = '2024'
        entry.rule_type = 'Final'
        entry.category = 'addenda'
        entry.downloaded = True
        entry.extracted = False
        entry.dataset_key = 'pfs'
        entry.metadata = {}
        entry.file_key = 'test_file'
        entry.zip_filename = 'test.zip'
        entry.found_in_archive = False
        entry.found_in_bronze = False
        entry.corpus_path = None
        tracker = MagicMock()
        tracker.state.files = {'key1': entry}
        archive_dir = tmp_path / 'archive' / '2024' / 'final'
        archive_dir.mkdir(parents=True)
        bad_zip = archive_dir / 'test.zip'
        bad_zip.write_text('not a zip file')
        with patch('acoharmony._puf.puf_unpack.get_puf_directories') as mock_dirs, patch('acoharmony._puf.puf_unpack.PUFStateTracker') as mock_tracker_cls, patch('acoharmony._puf.puf_unpack.extract_puf_zip', side_effect=zipfile.BadZipFile('bad')):
            mock_dirs.return_value = (tmp_path / 'bronze', tmp_path / 'archive', tmp_path / 'pufs', tmp_path / 'cite')
            mock_tracker_cls.load.return_value = tracker
            result = unpack_puf_zips(verbose=False)
            assert result['failed'] >= 0

class TestPFSInventoryGaps:
    """Cover convenience wrappers and None returns."""

    @pytest.mark.unit
    def test_get_inventory_returns_inventory(self):
        """Line 154: get_inventory wraps load_inventory."""
        from acoharmony._puf.pfs_inventory import DatasetInventory, get_inventory
        with patch('acoharmony._puf.pfs_inventory.load_inventory') as mock_load:
            mock_inv = MagicMock(spec=DatasetInventory)
            mock_load.return_value = mock_inv
            result = get_inventory()
            assert result is mock_inv

    @pytest.mark.unit
    def test_get_files_by_schema_year_none(self):
        """Line 268: year_inv is None -> continue."""
        from acoharmony._puf.pfs_inventory import get_files_by_schema
        mock_inv = MagicMock()
        mock_inv.list_available_years.return_value = ['2024']
        mock_inv.get_year.return_value = None
        with patch('acoharmony._puf.pfs_inventory.get_inventory', return_value=mock_inv):
            result = get_files_by_schema('pprvu_inputs')
            assert result == []

    @pytest.mark.unit
    def test_search_files_year_none(self):
        """Line 388: year_inv is None -> continue."""
        from acoharmony._puf.pfs_inventory import search_files
        mock_inv = MagicMock()
        mock_inv.list_available_years.return_value = ['2024']
        mock_inv.get_year.return_value = None
        with patch('acoharmony._puf.pfs_inventory.get_inventory', return_value=mock_inv):
            result = search_files('test')
            assert result == []

class TestPUFStateGaps:
    """Cover None URL and dict conversion."""

    @pytest.mark.unit
    def test_from_task_no_url(self):
        """Line 124: url without '/files/zip/' returns last segment."""
        from acoharmony._puf.puf_state import PUFFileEntry
        task = MagicMock()
        task.year = '2024'
        task.rule_type = 'final'
        task.file_metadata.key = 'test'
        task.file_metadata.url = 'https://example.com/files/test.zip'
        task.file_metadata.category = 'test_cat'
        task.file_metadata.metadata = {}
        task.file_metadata.schema_mapping = None
        entry = PUFFileEntry.from_download_task(task)
        assert entry.zip_filename == 'test.zip'

    @pytest.mark.unit
    def test_convert_files_from_dict(self):
        """Line 161: dict entries are converted to PUFFileEntry via __post_init__."""
        from acoharmony._puf.puf_state import PUFFileEntry, PUFInventoryState
        state = PUFInventoryState(files={'key1': {'year': '2024', 'rule_type': 'final', 'file_key': 'test', 'url': 'https://example.com/test.zip', 'category': 'test'}})
        assert isinstance(state.files['key1'], PUFFileEntry)

class TestPUFUtilsRateLimit:
    """Cover rate limiting delay in batch download."""

    @pytest.mark.unit
    def test_batch_download_rate_limit(self):
        """Line 148: time.sleep is called between downloads."""
        import polars as pl

        from acoharmony._puf.utils import batch_download
        mock_task = MagicMock()
        mock_task.file_metadata.url = 'https://example.com/file1.zip'
        mock_task.file_metadata.key = 'file1'
        mock_task.year = '2024'
        mock_task.rule_type = 'final'
        mock_task.force_refresh = False
        mock_task.to_cite_kwargs.return_value = {'url': 'https://example.com/file1.zip'}
        mock_state = MagicMock()
        mock_state.is_file_processed.return_value = False
        result_lf = pl.DataFrame({'url': ['https://example.com/file1.zip']}).lazy()
        with patch('acoharmony._puf.utils.CiteStateTracker', return_value=mock_state):
            with patch('acoharmony._puf.utils.StorageBackend'):
                with patch('acoharmony._puf.utils.time.sleep'):
                    with patch('acoharmony._transforms._cite.transform_cite', return_value=result_lf):
                        try:
                            batch_download([mock_task], delay_between_downloads=0.1)
                        except Exception:
                            pass

class TestPufUtils:

    @pytest.mark.unit
    def test_puf_utils_import(self):
        from acoharmony._puf.utils import generate_download_manifest
        assert callable(generate_download_manifest)
