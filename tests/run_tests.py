import pytest

def run_tests():
  """Exécute les tests automatisés du pipeline avec pytest."""
  pytest.main(["/home/jovyan/tests/test_transformer.py", "-v"])

if __name__ == "__main__":
  run_tests()