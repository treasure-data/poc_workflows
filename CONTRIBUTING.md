# Contributing to Treasure Data POC Workflows

Thank you for your interest in contributing to this project! This document provides guidelines and best practices for contributing to the Treasure Data POC workflows repository.

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Git
- Treasure Data account and API key for testing

### Development Setup

1. **Fork and clone the repository:**
   ```bash
   git clone https://github.com/your-username/poc_workflows.git
   cd poc_workflows
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r common/requirements.txt
   pip install -r tests/requirements.txt
   ```

4. **Run tests to ensure everything is working:**
   ```bash
   python -m pytest tests/ -v
   ```

## 📋 Development Guidelines

### Code Style

- **Follow PEP 8** for Python code style
- **Use Black** for code formatting: `black .`
- **Use isort** for import sorting: `isort .`
- **Maximum line length:** 100 characters
- **Use type hints** for function signatures

### Code Quality

- **Write comprehensive docstrings** for all functions and classes
- **Include error handling** for all external API calls
- **Use structured logging** instead of print statements
- **Follow the DRY principle** - avoid code duplication
- **Write unit tests** for new functionality

### Architecture Principles

- **Use the common module** for shared functionality
- **Implement proper error handling** with custom exceptions
- **Include retry logic** for network operations
- **Validate inputs** and provide clear error messages
- **Support both US and EU regions** for Treasure Data APIs

## 🧪 Testing

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ -v --cov=common --cov-report=html

# Run specific test file
python -m pytest tests/test_td_api_client.py -v
```

### Writing Tests

- **Write unit tests** for all new functions
- **Use mocking** for external API calls
- **Test error conditions** and edge cases
- **Aim for >80% code coverage**
- **Include integration tests** for complex workflows

### Test Structure

```python
class TestMyFeature(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        pass
    
    def test_successful_operation(self):
        """Test the happy path."""
        pass
    
    def test_error_handling(self):
        """Test error conditions."""
        pass
```

## 📝 Documentation

### Code Documentation

- **Write clear docstrings** following Google or NumPy format
- **Include parameter types** and return types
- **Document exceptions** that can be raised
- **Provide usage examples** in docstrings

### README Updates

- **Update README.md** for new features
- **Include configuration examples**
- **Document new environment variables**
- **Add usage examples**

## 🔄 Pull Request Process

### Before Submitting

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Run the full test suite:**
   ```bash
   python -m pytest tests/ -v
   flake8 .
   black --check .
   isort --check-only .
   ```

3. **Update documentation** as needed

4. **Add tests** for new functionality

### PR Requirements

- **Descriptive title** and detailed description
- **Link to related issues** if applicable
- **Include test coverage** for new code
- **Pass all CI checks**
- **Request review** from maintainers

### PR Template

```markdown
## Description
Brief description of changes made.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing
- [ ] All tests pass
- [ ] New tests added for new functionality
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Changes generate no new warnings
```

## 🐛 Bug Reports

### Creating Bug Reports

Use the following template for bug reports:

```markdown
**Bug Description**
A clear description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Set environment variables...
2. Run command...
3. See error

**Expected Behavior**
What you expected to happen.

**Actual Behavior**
What actually happened.

**Environment**
- OS: [e.g., Ubuntu 20.04]
- Python version: [e.g., 3.9.7]
- Treasure Data region: [e.g., US/EU]

**Additional Context**
Any other context or logs.
```

## 💡 Feature Requests

### Proposing Features

1. **Check existing issues** to avoid duplicates
2. **Provide clear use case** and motivation
3. **Describe the proposed solution**
4. **Consider backward compatibility**
5. **Include implementation ideas** if you have them

## 🔒 Security

### Security Best Practices

- **Never commit API keys** or sensitive data
- **Use environment variables** for configuration
- **Validate all inputs** to prevent injection attacks
- **Use HTTPS** for all API communications
- **Follow principle of least privilege**

### Reporting Security Issues

For security vulnerabilities, please email security@treasuredata.com instead of creating a public issue.

## 📚 Resources

### Useful Links

- [Treasure Data API Documentation](https://docs.treasuredata.com/display/public/PD/APIs)
- [Python PEP 8 Style Guide](https://pep8.org/)
- [pytest Documentation](https://docs.pytest.org/)
- [Black Code Formatter](https://black.readthedocs.io/)

### Learning Resources

- [Python Testing 101](https://realpython.com/python-testing/)
- [Writing Better Python Code](https://realpython.com/tutorials/best-practices/)
- [Git Workflow Best Practices](https://www.atlassian.com/git/tutorials/comparing-workflows)

## 📞 Getting Help

- **Create an issue** for bugs or feature requests
- **Start a discussion** for questions or ideas
- **Review existing documentation** and code examples
- **Contact maintainers** for urgent issues

## 🎉 Recognition

Contributors will be recognized in:
- **Contributors section** of README.md
- **Release notes** for significant contributions
- **Project documentation** for major features

Thank you for helping make this project better! 🚀
