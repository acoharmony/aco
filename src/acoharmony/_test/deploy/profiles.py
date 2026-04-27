# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_profiles.py."""


from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestProfileServiceMapper:
    @pytest.mark.unit
    def test_local_profile_init(self) -> None:
        mapper = ProfileServiceMapper("local")
        assert mapper.profile == "local"
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
        assert ProfileServiceMapper("prod").groups == {}

    @pytest.mark.unit
    def test_unknown_profile_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown profile: 'badprofile'"):
            ProfileServiceMapper("badprofile")

    @pytest.mark.unit
    def test_unknown_profile_lists_available(self) -> None:
        with pytest.raises(ValueError, match="Available profiles:"):
            ProfileServiceMapper("nope")


class TestProfileServiceMapperGetGroupServices:
    @pytest.mark.unit
    def test_valid_group(self) -> None:
        mapper = ProfileServiceMapper("dev")
        assert mapper.get_group_services("analytics") == ["marimo", "docs"]

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
    @pytest.mark.unit
    def test_local_all_services(self) -> None:
        all_svc = ProfileServiceMapper("local").get_all_services()
        assert all_svc == ["docs", "marimo"]

    @pytest.mark.unit
    def test_dev_all_services(self) -> None:
        all_svc = ProfileServiceMapper("dev").get_all_services()
        assert all_svc == sorted(["4icli", "aco", "marimo", "docs"])

    @pytest.mark.unit
    def test_prod_has_no_services(self) -> None:
        assert ProfileServiceMapper("prod").get_all_services() == []

    @pytest.mark.unit
    def test_returns_sorted_unique(self) -> None:
        all_svc = ProfileServiceMapper("dev").get_all_services()
        assert all_svc == sorted(set(all_svc))


class TestProfileServiceMapperListGroups:
    @pytest.mark.unit
    def test_local_groups(self) -> None:
        assert ProfileServiceMapper("local").list_groups() == ["analytics"]

    @pytest.mark.unit
    def test_dev_groups(self) -> None:
        groups = ProfileServiceMapper("dev").list_groups()
        assert groups == sorted(["analytics", "infrastructure"])

    @pytest.mark.unit
    def test_prod_groups_empty(self) -> None:
        assert ProfileServiceMapper("prod").list_groups() == []


class TestProfileServiceMapperIsProduction:
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
    @pytest.mark.unit
    def test_all_valid(self) -> None:
        mapper = ProfileServiceMapper("dev")
        valid, invalid = mapper.validate_services(["4icli", "aco"])
        assert valid == ["4icli", "aco"]
        assert invalid == []

    @pytest.mark.unit
    def test_all_invalid(self) -> None:
        mapper = ProfileServiceMapper("local")
        valid, invalid = mapper.validate_services(["nosvc1", "nosvc2"])
        assert valid == []
        assert invalid == ["nosvc1", "nosvc2"]

    @pytest.mark.unit
    def test_mixed(self) -> None:
        mapper = ProfileServiceMapper("dev")
        valid, invalid = mapper.validate_services(["4icli", "fake"])
        assert valid == ["4icli"]
        assert invalid == ["fake"]

    @pytest.mark.unit
    def test_prod_all_invalid(self) -> None:
        mapper = ProfileServiceMapper("prod")
        valid, invalid = mapper.validate_services(["4icli"])
        assert valid == []
        assert invalid == ["4icli"]

    @pytest.mark.unit
    def test_empty_list(self) -> None:
        mapper = ProfileServiceMapper("dev")
        valid, invalid = mapper.validate_services([])
        assert valid == []
        assert invalid == []
