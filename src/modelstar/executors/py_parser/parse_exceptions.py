

class UDFTypeError(Exception):
    """Exception raised for UDF type declaration errors.

    Attributes:
        file_name     : Name of the file where the function is located.
        function_name : Name of the function.
        line_no       : Line number of the function in the file. 
        type          : Type of error.
    """

    def __init__(self, file_name: str, function_name: str, line_no: int, type: str):
        self.file_name = file_name
        self.function_name = function_name
        self.line_no = line_no
        self.type = type
        self.message = f'Typing error for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        super().__init__(self.message)

    def __str__(self):
        if self.type == 'missing':
            message = f'Types for function argument missing for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        elif self.type == 'not-base':
            message = f'Use only accepted Python types for UDF functions. Error for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        else:
            message = f'Typing error for `{self.function_name}` in `{self.file_name}` at line no. {self.line_no}.'
        return message
