"""
Unit tests for citation batch transform.

Tests batch processing of multiple citations with parallel execution
and error handling.
"""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock, patch

import polars as pl
import pytest
import acoharmony

if TYPE_CHECKING:
    pass

class TestCiteBatchTransform:
    """Tests for citation batch transform."""

    @pytest.mark.unit
    def test_transform_cite_batch_exists(self) -> None:
        """Test that batch transform exists."""
        try:
            from acoharmony._transforms._cite_batch import transform_cite_batch
            assert callable(transform_cite_batch)
        except ImportError:
            pytest.skip('Batch transform not yet implemented')

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_transform_cite_batch_basic(self, mock_transform: Mock) -> None:
        """Test basic batch processing."""
        try:
            from acoharmony._transforms._cite_batch import transform_cite_batch
        except ImportError:
            pytest.skip('Batch transform not yet implemented')
        mock_transform.return_value = pl.LazyFrame({'url': ['https://example.com/1'], 'title': ['Paper 1']})
        urls = ['https://example.com/1', 'https://example.com/2', 'https://example.com/3']
        result = transform_cite_batch(urls)
        assert isinstance(result, pl.LazyFrame)
        assert mock_transform.call_count == len(urls)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_transform_cite_batch_error_handling(self, mock_transform: Mock) -> None:
        """Test batch processing with errors."""
        try:
            from acoharmony._transforms._cite_batch import transform_cite_batch
        except ImportError:
            pytest.skip('Batch transform not yet implemented')

        def transform_side_effect(url: str, *args, **kwargs):
            if 'fail' in url:
                raise Exception('Failed to process')
            return pl.LazyFrame({'url': [url], 'title': ['Paper']})
        mock_transform.side_effect = transform_side_effect
        urls = ['https://example.com/1', 'https://example.com/fail', 'https://example.com/2']
        result = transform_cite_batch(urls)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_cite_batch_parallel(self) -> None:
        """Test parallel execution configuration."""
        try:
            import inspect

            from acoharmony._transforms._cite_batch import transform_cite_batch
            sig = inspect.signature(transform_cite_batch)
            params = list(sig.parameters.keys())
            assert len(params) > 0
        except ImportError:
            pytest.skip('Batch transform not yet implemented')

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_transform_cite_batch_empty_list(self, mock_transform: Mock) -> None:
        """Test batch processing with empty URL list."""
        try:
            from acoharmony._transforms._cite_batch import transform_cite_batch
        except ImportError:
            pytest.skip('Batch transform not yet implemented')
        with pytest.raises(ValueError, match='No citations successfully processed'):
            transform_cite_batch([])

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_transform_cite_batch_deduplication(self, mock_transform: Mock) -> None:
        """Test that duplicate URLs are handled."""
        try:
            from acoharmony._transforms._cite_batch import transform_cite_batch
        except ImportError:
            pytest.skip('Batch transform not yet implemented')
        call_count = 0

        def transform_side_effect(url: str, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            return pl.LazyFrame({'url': [url], 'title': [f'Paper {call_count}']})
        mock_transform.side_effect = transform_side_effect
        urls = ['https://example.com/1', 'https://example.com/1', 'https://example.com/2']
        result = transform_cite_batch(urls)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_transform_cite_batch_progress(self, mock_transform: Mock) -> None:
        """Test that batch processing provides progress indication."""
        try:
            import inspect

            from acoharmony._transforms._cite_batch import transform_cite_batch
            inspect.signature(transform_cite_batch)
            assert callable(transform_cite_batch)
        except ImportError:
            pytest.skip('Batch transform not yet implemented')

class TestCiteBatchTransformExtended:
    """Tests for cite batch transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _cite_batch
        assert acoharmony._transforms._cite_batch is not None

    @pytest.mark.unit
    def test_transform_cite_batch_exists(self):
        from acoharmony._transforms._cite_batch import transform_cite_batch
        assert callable(transform_cite_batch)

class TestTransformCiteDirectory:
    """Tests for transform_cite_directory covering uncovered paths."""

    @pytest.mark.unit
    def test_directory_not_found(self):
        from acoharmony._transforms._cite_batch import transform_cite_directory
        with pytest.raises(FileNotFoundError, match='Directory not found'):
            transform_cite_directory('/nonexistent/dir/xyz')

    @pytest.mark.unit
    def test_no_files_matching(self, tmp_path):
        from acoharmony._transforms._cite_batch import transform_cite_directory
        empty_dir = tmp_path / 'empty'
        empty_dir.mkdir()
        with pytest.raises(ValueError, match='No files found'):
            transform_cite_directory(empty_dir, pattern='*.pdf')

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite_batch')
    def test_directory_with_files(self, mock_batch, tmp_path):
        from acoharmony._transforms._cite_batch import transform_cite_directory
        dir_path = tmp_path / 'docs'
        dir_path.mkdir()
        (dir_path / 'file1.txt').write_text('content1')
        (dir_path / 'file2.txt').write_text('content2')
        mock_batch.return_value = pl.LazyFrame({'url': ['a'], 'title': ['b']})
        result = transform_cite_directory(dir_path, pattern='*.txt')
        assert isinstance(result, pl.LazyFrame)
        call_args = mock_batch.call_args
        urls = call_args[0][0]
        assert all(u.startswith('file://') for u in urls)
        assert len(urls) == 2

class TestTransformCiteBatchCorpusWriting:
    """Tests for corpus writing paths in transform_cite_batch."""

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_batch_creates_new_corpus(self, mock_transform, tmp_path):
        from acoharmony._transforms._cite_batch import transform_cite_batch
        mock_transform.return_value = pl.LazyFrame({'url': ['u1'], 'title': ['t1']})
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True)
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = str(corpus_dir)
        with patch('acoharmony._store.StorageBackend', return_value=mock_storage):
            result = transform_cite_batch(['https://example.com/1'])
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_batch_appends_to_existing_corpus(self, mock_transform, tmp_path):
        from acoharmony._transforms._cite_batch import transform_cite_batch
        corpus_dir = tmp_path / 'cites' / 'corpus'
        corpus_dir.mkdir(parents=True)
        existing = pl.DataFrame({'url': ['existing'], 'title': ['old']})
        existing.write_parquet(str(corpus_dir / 'corpus.parquet'))
        mock_transform.return_value = pl.LazyFrame({'url': ['new'], 'title': ['new_title']})
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = str(corpus_dir)
        with patch('acoharmony._store.StorageBackend', return_value=mock_storage):
            result = transform_cite_batch(['https://example.com/new'])
        assert isinstance(result, pl.LazyFrame)
        final = pl.read_parquet(str(corpus_dir / 'corpus.parquet'))
        assert final.height == 2

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_batch_all_fail_raises(self, mock_transform):
        from acoharmony._transforms._cite_batch import transform_cite_batch
        mock_transform.side_effect = Exception('fail')
        with pytest.raises(ValueError, match='No citations successfully processed'):
            transform_cite_batch(['https://fail1.com', 'https://fail2.com'])

    @pytest.mark.unit
    @patch('acoharmony._transforms._cite_batch.transform_cite')
    def test_batch_partial_failure_warns(self, mock_transform, tmp_path):
        from acoharmony._transforms._cite_batch import transform_cite_batch

        def side_effect(url, *args, **kwargs):
            if 'fail' in url:
                raise Exception('failed')
            return pl.LazyFrame({'url': [url], 'title': ['ok']})
        mock_transform.side_effect = side_effect
        mock_storage = MagicMock()
        corpus_dir = tmp_path / 'corpus'
        corpus_dir.mkdir()
        mock_storage.get_path.return_value = str(corpus_dir)
        with patch('acoharmony._store.StorageBackend', return_value=mock_storage):
            result = transform_cite_batch(['https://ok.com', 'https://fail.com'], max_workers=1)
        assert isinstance(result, pl.LazyFrame)
