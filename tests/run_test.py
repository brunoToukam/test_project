import unittest

if __name__ == '__main__':
    testsuite = unittest.TestLoader().discover(start_dir='tests', pattern='test_*.py')
    unittest.TextTestRunner(verbosity=2).run(testsuite)
