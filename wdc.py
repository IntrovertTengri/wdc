import requests

# database connection object
class dbc:
    # initalizing our dbc by providing it with the service endpoint, from which we can get a datacube
    def __init__(self, url):
        """
        Initializes a new dbc instance which is used to manage connections and send queries to a WCPS server.

        Parameters:
            url (str): The endpoint URL of the WCPS server.

        Example:
            >>> database_connection = dbc("https://ows.rasdaman.org/rasdaman/ows")
        """
        self.server_url = url
    
    def send_query(self, wcps_query):
        """
        Sends a WCPS query to the server and retrieves the response.

        Parameters:
            wcps_query (str): A string containing the WCPS query.

        Returns:
            Response: A response object from the requests library containing the server's response to the query.

        Example:
            >>> result = database_connection.send_query("for c in (AvgLandTemp) return encode(c, 'csv')")
        """
        # getting a response from the server
        try:
            response = requests.post(self.server_url, data = {'query': wcps_query}, verify = False)
            return response

        except requests.exceptions.RequestException as e:
            print(f'Error: {e}')



# function needed for converting a byte string to the list of numbers
def byte_to_list(byte_str):
    """
    Converts a byte string into a list of floats. Useful for parsing numeric data returned from a server.

    Parameters:
        byte_str (bytes): The byte string to be converted.

    Returns:
        list of float: A list of floats derived from the byte string.

    Example:
        >>> byte_to_list(b'1.0,2.0,3.0')
        [1.0, 2.0, 3.0]
    """
    decoded_str = byte_str.decode('utf-8') # decode the byte string
    str_list = decoded_str.split(',') # split numbers separated by comma
    num_list = [float(num) for num in str_list] # create a list of numbers
    return num_list


# datacube object
class dco:
    # initializing the dco
    def __init__(self, dbc_being_used):
        """
        Initializes a dco instance which manages WCPS queries using a dbc object for server communication.

        Parameters:
            dbc_being_used (dbc): An instance of dbc used for server communication.

        Example:
            >>> datacube = dco(database_connection)
        """
        self.DBC = dbc_being_used
        # default values
        self.vars = []
        self.Subsets = []
        self.aggregation = None
        self.format = None
        self.aggregation_condition = None
        self.filter_condition = None
        self.var_names = []
        self.transformation = None
        self.encode_as = None
        
    def reset(self):
        """
        Resets the attributes of the dco instance to their default values, except for the dbc connection
            which remains unchanged.

        Returns:
            self: Returns the instance itself with reset values.

        Example:
            >>> datacube.reset()
        """
        self.vars = []
        self.Subsets = []
        self.aggregation = None
        self.format = None
        self.count_condition = None
        self.filter_condition = None
        self.var_names = []
        self.transformation = None
        self.encode_as = None
        return self
    
    def get_all_var_names(self, string):
        """
        Extracts all variable names from a string where variables are prefixed by '$' and can be
            followed by various delimiters such as spaces, commas, parentheses, etc.

        Parameters:
            string (str): A string potentially containing multiple variables each prefixed by '$'.

        Returns:
            list: A list of extracted variable names. Returns an empty list if no variables are found.

        Example:
            >>> var_names = get_all_var_names("$a>15 and $b")
            >>> print(var_names)
            ['$a', '$b']
        """
        var_names = []
        index = 0
        while index < len(string):
            start_index = string.find('$', index)
            if start_index == -1:
                break 

            delimiters = [' ', ',', '(', ')', '[', ']', '{', '}', ';', '>', '<', '+', '-', '=', '.', 
                         '/', '\\', '|', '!']
            end_index = len(string)

            for i, char in enumerate(string[start_index:]):
                if char in delimiters:
                    end_index = start_index + i
                    break

            var_names.append(string[start_index:end_index])
            index = end_index
            
        if len(var_names) == 0:
            return None
        return var_names

    
    def initialize_var(self, s):
        """
        Initializes a variable for the datacube object. This method extracts the variable name from 
            the input,checks if it's successfully defined, and then appends the variable along with
            its associated datacube to the respective lists within the dco instance.

        Parameters:
            s (str): A string that combines the variable name and its associated datacube,
                formatted as '$variable_name in (coverage_name)'.

        Returns:
            self: Returns the instance itself if the variable is successfully added.
            
        Raises:
            ValueError: If the format of the variable initialization wasn't correct  

        Example:
            >>> datacube.initialize_var("$c in (AvgLandTemp)")
        """
        if not(s.startswith('$') and " in (" in s and s.endswith(')')):
            raise ValueError("The format of variable initialization wasn't correct")
        
        var_name = self.get_all_var_names(s)[0]
        self.vars.append(s)
        self.Subsets.append(None)
        self.var_names.append(var_name)
        return self
    
    def do_vars_exist(self, string):
        """
        Checks whether all variable names extracted from the input string exist in the predefined list of
        variable names of the current instance. This method is useful for validating that variables
        referenced in a string (e.g., a query or command) are all recognized by the system
        before proceeding with further operations.

        Parameters:
            string (str): The string from which variable names are extracted and checked.
                Variables in the string should be prefixed by '$'.

        Returns:
            bool: True if all extracted variables exist in the instance's variable list.

        Raises:
            ValueError: If no variables are specified in the string, or
            if one or more variables do not exist in the instance's list of variables.

        Example:
            >>> datacube.do_vars_exist("$temp and $pressure")
            True
            >>> datacube.do_vars_exist("$humidity")
            Traceback (most recent call last):
            ...
            ValueError: Variables in a string don't exist
        """
        string = string.replace('\n', ' ')
        string = string.replace('\r', ' ')
        string = string.replace('\t', ' ')
        var_names = self.get_all_var_names(string)
        if var_names != None:
            var_names = set(var_names)
            if var_names.issubset(set(self.var_names)):
                return True
            else:
                raise ValueError("Variables in a string don't exist")
        else:
            raise ValueError("Variables weren't specified")
        
        
    def subset(self, subset, var_name):
        """
        Adds a subset specification for a specific variable in the datacube object. This method
            identifies the variable by its name, then associates the specified subset with it, updating
            the internal state of the dco instance.

        Parameters:
            subset (str): The subset condition to apply, typically specifying a range or filter for the
                data retrieval.
            var_name (str): The name of the variable to which the subset will be applied. This variable
                must already be initialized in the dco instance.

        Returns:
            self: Returns the instance itself after updating the subset for the specified variable,
                allowing for method chaining.

        Example:
            >>> datacube.subset(var_name = '$c',
                subset = 'Lat(53.08), Long(8.80), ansi("2014-01":"2014-12")')
        """
        if not(var_name in self.var_names):
            raise ValueError("Such variable doesn't exist")
        idx = self.var_names.index(var_name)
        self.Subsets[idx] = subset
        return self
        
    def where(self, filter_condition):
        """
        Sets a filter condition for the datacube query. This method allows specifying conditions that
            filter the data according to certain criteria, similar to a SQL WHERE clause. The specified
            condition will be applied when the query is executed to filter the results.

        Parameters:
            filter_condition (str): A string representing the condition to be applied to filter the data.
                This condition should be formatted according to the expected WCPS syntax.

        Returns:
            self: Returns the instance itself, allowing for method chaining and further configuration.

        Example:
            >>> datacube.where("$c > 20 and $c < 50")
        """
        self.do_vars_exist(filter_condition)
        self.filter_condition = filter_condition
        return self
    

    def set_format(self, output_format):
        """
        Sets the output format for data retrieved from the datacube. This method allows the user to specify 
        the format in which the data should be returned after a query is executed, enabling different
        types of data processing.

        Parameters:
            output_format (str): The format to set for output data. Valid options are "CSV", "PNG", or "JPEG".

        Returns:
            self: Returns the instance itself after setting the output format, allowing for method chaining.

        Example:
            >>> datacube.set_format("PNG")
        """
        self.format = output_format
        return self
    
        
    # aggregation methods
    def min(self, condition = None):
        """
        Configures the datacube to compute the minimum value of the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.

        Parameters:
            condition (str, optional): A condition that defines the subset of data for
                which the minimum is calculated.

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.min("$c > 20")
        """
        if condition != None:
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'MIN'
        return self
        
    def max(self, condition = None):
        """
        Configures the datacube to compute the maximum value of the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.

        Parameters:
            condition (str, optional): A condition that defines the subset of data for
                which the minimum is calculated.

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.max("$c > 20")
        """
        if condition != None:
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'MAX'
        return self
    
    def avg(self, condition = None):
        """
        Configures the datacube to compute the average value of the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.

        Parameters:
            condition (str, optional): A condition that defines the subset of data for
                which the minimum is calculated.

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.avg("$c > 20")
        """
        if condition != None:
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'AVG'
        return self
    
    def sum(self, condition = None):
        """
        Configures the datacube to compute the sum of values across the specified data subset when executed.
            An optional condition can specify the subset or criteria for the aggregation.

        Parameters:
            condition (str, optional): A condition that defines the subset of data for
                which the minimum is calculated.

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.sum("$c > 20")
        """
        if condition != None:
            self.do_vars_exist(condition)
        self.aggregation_condition = condition
        self.aggregation = 'SUM'
        return self
        
    def count(self, condition = None):
        """
        Configures the datacube to count the number of data points that meet the specified
            condition when executed.

        Parameters:
            condition (str, optional): A condition that specifies the criteria that data
                points must meet to be counted.

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.count("$c > 20")
        """
        if condition != None:
            self.do_vars_exist(condition)
        self.aggregation = 'COUNT'
        self.aggregation_condition = condition
        return self
    
    def replace_variables_with_subsets(self, str_to_transform = None):
        """
        Replaces variables in a given string with their corresponding subsets if defined.
            If no string is provided, it constructs a string representation of
            all variables and their subsets.

        Parameters:
            str_to_transform (str, optional):
                A string containing variable names that need to be replaced with their subsets.

        Returns:
            str: A new string with variables replaced by their subsets,
                or a concatenated string of all variables and subsets.

        Example:
            >>> datacube.replace_variables_with_subsets("abs($c - 200)")
            >>> "abs($c[corresponding_subset] - 200)"
        """
        if str_to_transform != None:
            expression = str_to_transform
            for var, subset in zip(self.var_names, self.Subsets):
                if subset != None:
                    expression = expression.replace(var, f'{var}[{subset}]')
            return expression
        else:
            expression = ''
            for var, subset in zip(self.var_names, self.Subsets):
                if subset != None:
                    expression += f'''{var}[{subset}] '''
                else:
                    expression += f'''{var}'''
            return expression
    
    def transform_data(self, operation):
        """
        Sets a transformation operation to be applied to the datacube when the query is executed.

        Parameters:
            operation (str): A string representing the transformation operation to be applied.

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.transform_data("abs($c - 3.6 * $c)")
        """
        self.do_vars_exist(operation)
        self.transformation = operation
        return self
    
    def encode(self, operation):
        """
        Specifies the encoding operation to be applied to the output of the query.

        Parameters:
            operation (str): A string representing the encoding function, such as "encode".

        Returns:
            self: Returns the instance itself, allowing for method chaining.

        Example:
            >>> datacube.encode("
                switch 
                    case $c = 99999 
                        return {red: 255; green: 255; blue: 255} 
                    case 18 > $c
                        return {red: 0; green: 0; blue: 255} 
                    case 23 > $c
                        return {red: 255; green: 255; blue: 0} 
                    case 30 > $c
                        return {red: 255; green: 140; blue: 0} 
                    default return {red: 255; green: 0; blue: 0}
            ")
        """
        self.do_vars_exist(operation)
        self.encode_as = operation
        return self
        
            
    # method, which returns the result after using aggregation functions
    def aggregate_data(self):
        """
        Constructs a part of the WCPS query for performing aggregation functions based on the current settings.

        Returns:
            str: A string representing the aggregation part of the WCPS query.

        Example:
            >>> query_part = datacube.aggregate_data()
        """
        helper_query = self.replace_variables_with_subsets(self.aggregation_condition)
        if self.aggregation == 'MIN':
            query = f'''min({helper_query})'''
        elif self.aggregation == 'AVG':
            query = f'''avg({helper_query})'''
        elif self.aggregation == 'MAX':
            query = f'''max({helper_query})'''
        elif self.aggregation == 'SUM':
            query = f'''sum({helper_query})'''
        elif self.aggregation == 'COUNT':
            query = f'''count({helper_query})'''
        return query
    
    def return_format(self):
        """
        Determines the format for the output based on the configured settings of the datacube.

        Returns:
            str: A string indicating the desired output format for the WCPS query.

        Example:
            >>> output_format = datacube.return_format()
        """
        if self.format == 'CSV':
            query = "text/csv" # if the desired format of the output is text/csv
        elif self.format == 'PNG':
            query = "image/png" # if the desired format of the output is image/png:
        elif self.format == 'JPEG': 
            query = "image/jpeg" # if the desired format of the output is image/jpeg:
        return query
    
    
    # the function, which converts operations from the dco object to the query
    def to_wcps_query(self):
        """
        Constructs a WCPS query string from the current state of the dco instance.

        Returns:
            str: A string that represents the complete WCPS query based on the current state of the
                dco instance

        Example:
            >>> query = datacube.to_wcps_query()
        """
        query = '''for '''
        for var in self.vars:
            query += (var + '\n')
        
        if self.filter_condition != None:
            query += f'''where {self.filter_condition}\n'''
        query += f'''return \n'''
        
        if self.aggregation != None:
            query += self.aggregate_data()
            return query
        
        if self.encode_as != None:
            helper_query = self.replace_variables_with_subsets(self.encode_as)
        elif self.transformation != None:
            helper_query = self.replace_variables_with_subsets(self.transformation)
        else:
            helper_query = self.replace_variables_with_subsets()
        
        if self.format != None: # we check whether the format was specified
            query += f'''encode({helper_query}, "{self.return_format()}")'''
        else:
            query += f'''{helper_query}'''
        return query
    
    
    # executing, when all the operations were added
    def execute(self):
        """
        Executes the constructed WCPS query and processes the response based on the specified format.

        Returns:
            Varies: The processed data as per the requested format 
                (CSV as list, PNG/JPEG as image object, or list of numbers).

        Example:
            >>> output = datacube.execute()
        """
        wcps_query = self.to_wcps_query() # get a WCPS query
        response = self.DBC.send_query(wcps_query) # pass the WCPS query to the server and get a response
        if self.format == 'CSV': # if the format is CSV, convert binary string to the list of numbers
            data = byte_to_list(response.content) 
            self.reset() # returning the values of the dco instance to default
            return data
        elif self.format == 'PNG': # if the format is PNG, return the image
            self.reset() # returning the values of the dco instance to default
            return response.content
        elif self.format == 'JPEG': # if the format is JPEG, return the image
            self.reset() # returning the values of the dco instance to default
            return response.content
        else:
            self.reset()
            data = byte_to_list(response.content) 
            return data