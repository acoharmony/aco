"""Shared fixtures for table model tests."""

import dataclasses

import pytest


def create_instance_bypassing_validation(cls):
    """Create an instance of a Pydantic dataclass bypassing all validators.

    Sets all fields to None so dataclasses.asdict() works without errors.
    """
    obj = cls.__new__(cls)
    for f in dataclasses.fields(cls):
        object.__setattr__(obj, f.name, None)
    return obj
