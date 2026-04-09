# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_profiles.py."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestProfileServiceMapper:
    """Tests for ProfileServiceMapper."""

    @pytest.mark.unit
    def test_local_profile_init(self) -> None:
        mapper = ProfileServiceMapper("local")
        assert mapper.profile == "local"
        assert "infrastructure" in mapper.groups
        assert "analytics" in mapper.groups

    @pytest.mark.unit
    def test_dev_profile_has_all_groups(self) -> None:
        mapper = ProfileServiceMapper("dev")
        assert "infrastructure" in mapper.groups
        assert "analytics" in mapper.groups

    @pytest.mark.unit
    def test_staging_profile_matches_dev_structure(self) -> None:
        mapper = ProfileServiceMapper("staging")
        assert set(mapper.groups.keys()) == set(ProfileServiceMapper("dev").groups.keys())

    @pytest.mark.unit
    def test_prod_profile_empty_groups(self) -> None:
        mapper = ProfileServiceMapper("prod")
        assert mapper.groups == {}

    @pytest.mark.unit
    def test_unknown_profile_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown profile: 'badprofile'"):
            ProfileServiceMapper("badprofile")

    @pytest.mark.unit
    def test_unknown_profile_lists_available(self) -> None:
        with pytest.raises(ValueError, match="Available profiles:"):
            ProfileServiceMapper("nope")


class TestProfileServiceMapperGetGroupServices:
    """Tests for get_group_services()."""

    @pytest.mark.unit
    def test_valid_group(self) -> None:
        mapper = ProfileServiceMapper("local")
        infra = mapper.get_group_services("infrastructure")
        assert "postgres" in infra
        assert "s3api" in infra

    @pytest.mark.unit
    def test_invalid_group_raises(self) -> None:
        mapper = ProfileServiceMapper("local")
        with pytest.raises(ValueError, match="Group 'nonexistent' not found"):
            mapper.get_group_services("nonexistent")

    @pytest.mark.unit
    def test_invalid_group_shows_available(self) -> None:
        mapper = ProfileServiceMapper("local")
        with pytest.raises(ValueError, match="Available groups:"):
            mapper.get_group_services("bad")

    @pytest.mark.unit
    def test_prod_has_no_groups(self) -> None:
        mapper = ProfileServiceMapper("prod")
        with pytest.raises(ValueError, match="Group 'infrastructure' not found"):
            mapper.get_group_services("infrastructure")


class TestProfileServiceMapperGetAllServices:
    """Tests for get_all_services()."""

    @pytest.mark.unit
    def test_local_all_services(self) -> None:
        mapper = ProfileServiceMapper("local")
        all_svc = mapper.get_all_services()
        assert "postgres" in all_svc
        assert "s3api" in all_svc
        assert "marimo" in all_svc
        assert "docs" in all_svc
        assert all_svc == sorted(all_svc)

    @pytest.mark.unit
    def test_dev_all_services_count(self) -> None:
        mapper = ProfileServiceMapper("dev")
        all_svc = mapper.get_all_services()
        # dev has infrastructure (4) + analytics (2) = 6
        assert len(all_svc) == 6

    @pytest.mark.unit
    def test_prod_has_no_services(self) -> None:
        mapper = ProfileServiceMapper("prod")
        assert mapper.get_all_services() == []

    @pytest.mark.unit
    def test_returns_sorted_unique(self) -> None:
        mapper = ProfileServiceMapper("dev")
        all_svc = mapper.get_all_services()
        assert all_svc == sorted(set(all_svc))


class TestProfileServiceMapperListGroups:
    """Tests for list_groups()."""

    @pytest.mark.unit
    def test_local_groups(self) -> None:
        mapper = ProfileServiceMapper("local")
        groups = mapper.list_groups()
        assert groups == sorted(["infrastructure", "analytics"])

    @pytest.mark.unit
    def test_dev_groups(self) -> None:
        mapper = ProfileServiceMapper("dev")
        groups = mapper.list_groups()
        assert "infrastructure" in groups
        assert "analytics" in groups
        assert groups == sorted(groups)

    @pytest.mark.unit
    def test_prod_groups_empty(self) -> None:
        mapper = ProfileServiceMapper("prod")
        assert mapper.list_groups() == []


class TestProfileServiceMapperIsProduction:
    """Tests for is_production()."""

    @pytest.mark.unit
    def test_prod_is_production(self) -> None:
        assert ProfileServiceMapper("prod").is_production() is True

    @pytest.mark.unit
    def test_dev_not_production(self) -> None:
        assert ProfileServiceMapper("dev").is_production() is False

    @pytest.mark.unit
    def test_local_not_production(self) -> None:
        assert ProfileServiceMapper("local").is_production() is False

    @pytest.mark.unit
    def test_staging_not_production(self) -> None:
        assert ProfileServiceMapper("staging").is_production() is False


class TestProfileServiceMapperValidateServices:
    """Tests for validate_services()."""

    @pytest.mark.unit
    def test_all_valid(self) -> None:
        mapper = ProfileServiceMapper("local")
        valid, invalid = mapper.validate_services(["postgres", "s3api"])
        assert valid == ["postgres", "s3api"]
        assert invalid == []

    @pytest.mark.unit
    def test_all_invalid(self) -> None:
        mapper = ProfileServiceMapper("local")
        valid, invalid = mapper.validate_services(["nosvc1", "nosvc2"])
        assert valid == []
        assert invalid == ["nosvc1", "nosvc2"]

    @pytest.mark.unit
    def test_mixed(self) -> None:
        mapper = ProfileServiceMapper("local")
        valid, invalid = mapper.validate_services(["postgres", "fake"])
        assert valid == ["postgres"]
        assert invalid == ["fake"]

    @pytest.mark.unit
    def test_prod_all_invalid(self) -> None:
        mapper = ProfileServiceMapper("prod")
        valid, invalid = mapper.validate_services(["postgres"])
        assert valid == []
        assert invalid == ["postgres"]

    @pytest.mark.unit
    def test_empty_list(self) -> None:
        mapper = ProfileServiceMapper("dev")
        valid, invalid = mapper.validate_services([])
        assert valid == []
        assert invalid == []
