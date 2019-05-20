import unittest


class AlienTest(unittest.TestCase):

  def test_0_run(self):
    from jnius import autoclass
    Stack = autoclass('java.util.Stack')
    stack = Stack()
    stack.push("Hello")
    stack.push("World")

    assert str("World" == stack.pop())
    assert str("Hello" == stack.pop())
