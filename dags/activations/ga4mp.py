# TODO(b/265710315): Refactor activations logic to avoid code duplication once we have more activations. Maybe consider using singledispatch, see reference article: https://johschmidt42.medium.com/improve-your-conditional-logic-in-python-with-singledispatch-advanced-enumerations-9e0c96c5da9f

# TODO(b/): Implement GA4 MP activation function
def activate(input_data):
  print(input_data)


# TODO(b/): Implement GA4 MP input data query
def get_query(location):
  return "SELECT * from cp.`employee.json`"
