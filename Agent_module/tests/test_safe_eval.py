"""Unit tests for safe_eval — the AST-based replacement for eval()."""

from __future__ import annotations

import pytest

from Agent_module.safe_eval import safe_eval


class TestArithmetic:
    def test_addition(self):
        assert safe_eval("2 + 3") == 5

    def test_subtraction(self):
        assert safe_eval("10 - 7") == 3

    def test_multiplication(self):
        assert safe_eval("4 * 5") == 20

    def test_true_division(self):
        assert safe_eval("10 / 4") == 2.5

    def test_floor_division(self):
        assert safe_eval("10 // 3") == 3

    def test_modulo(self):
        assert safe_eval("10 % 3") == 1

    def test_power(self):
        assert safe_eval("2 ** 10") == 1024

    def test_parentheses(self):
        assert safe_eval("(2 + 3) * 4") == 20

    def test_unary_minus(self):
        assert safe_eval("-5 + 3") == -2

    def test_unary_plus(self):
        assert safe_eval("+5") == 5

    def test_float_literal(self):
        assert safe_eval("1.5 * 2") == 3.0

    def test_nested(self):
        assert safe_eval("((1 + 2) * 3 - 4) ** 2") == 25


class TestSecurityRejections:
    """Anything outside the arithmetic allowlist must raise ValueError."""

    @pytest.mark.parametrize("expr", [
        "__import__('os')",
        "open('/etc/passwd')",
        "print(1)",
        "a + 1",
        "os.system('ls')",
        "[1, 2, 3]",
        "{1: 2}",
        "lambda: 1",
        "1 if True else 2",
        "True and False",
        "1 == 1",
        "x := 1",
    ])
    def test_rejects_non_arithmetic(self, expr):
        with pytest.raises(ValueError):
            safe_eval(expr)

    def test_rejects_boolean_literal(self):
        # bool is a subclass of int — we must still reject it explicitly.
        with pytest.raises(ValueError):
            safe_eval("True")

    def test_rejects_string_literal(self):
        with pytest.raises(ValueError):
            safe_eval("'hello'")

    def test_syntax_error_becomes_value_error(self):
        with pytest.raises(ValueError):
            safe_eval("1 +")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            safe_eval("")


class TestArithmeticErrors:
    def test_divide_by_zero_raises(self):
        with pytest.raises(ZeroDivisionError):
            safe_eval("1 / 0")

    def test_floor_divide_by_zero_raises(self):
        with pytest.raises(ZeroDivisionError):
            safe_eval("1 // 0")
