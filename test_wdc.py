from wdc import dco, dbc
import pytest
import warnings
warnings.filterwarnings("ignore")

# this tests initialization of dbc() instance
class Test_init_dbc():
    # initialize dbc() instance correctly by passing a string
    def test_init_correctly(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        assert isinstance(my_dbc.server_url, str)

    # initialize dbc() instance by passing not a string
    def test_init_not_string(self):
        with pytest.raises(TypeError):
            my_dbc = dbc(2)

# this tests send_query()
class Test_send_query():
    # send incorrect query
    def test_send_wrong_query(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        with pytest.raises(Exception):
            my_dbc.send_query("for $c in")

    # send correct query
    def test_send_correct_query(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        response = my_dbc.send_query('for $c in (AvgLandTemp) return 1')
        assert (response.status_code == 200) and (response.content == b'1')

    # pass to the method a variable, which is not a string
    def test_send_not_str(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        with pytest.raises(TypeError):
            my_dbc.send_query(1)

# this tests initialization of dco() instance
class Test_init_dco():
    # init by not passing a dbc() instance
    def test_not_correct_dbc(self):
        with pytest.raises(TypeError):
            my_dco = dco(2)

    # init correct dbc() instance
    def test_pass_dbc(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        my_dco = dco(my_dbc)
        assert isinstance(my_dco.DBC, dbc)

# we will get coverages from the https://ows.rasdaman.org/rasdaman/ows
def create_dco():
    my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
    my_dco = dco(my_dbc)
    return my_dco

# this tests initialization of the variable in the dco()
class Test_init_var():
    # init_var gets a string in a correct format
    def test_good_format(self):
        my_dco = create_dco()
        assert isinstance(my_dco.initialize_var("$c in (AvgLandTemp)"), dco)
    
    # init_var gets not a string
    def test_type_error(self):
        my_dco = create_dco()
        with pytest.raises(TypeError):
            my_dco.initialize_var(42)
    
    # init_var gets a string in not correct format
    def test_format_error(self):
        my_dco = create_dco()
        with pytest.raises(ValueError):
            my_dco.initialize_var("$cin(AvgLandTemp)")


# this is a dco instance with a good initialized variable
def create_good_dco():
    my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
    my_dco = dco(my_dbc)
    return my_dco.initialize_var("$c in (AvgLandTemp)")

# this tests subset()
class Test_subset():
    # pass correct values as a subset
    def test_correct_subset(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.subset(var_name = '$c', subset = 'Lat(53.08), Long(8.80), ansi("2014-01":"2014-12")'), dco)
    
    # don't pass one of the arguments
    def test_dont_pass_one_arg(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.subset(var_name = '$c')

    # pass as an argument not a string to the var_name argument
    def test_pass_not_str_varname(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.subset(var_name = 2, subset = 'Lat(53.08), Long(8.80), ansi("2014-01":"2014-12")')
    
    # pass as an argument not a string to the subset argument
    def test_pass_not_str_subset(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.subset(var_name = '$c', subset = 1000)

    # pass as an argument not existing variable
    def test_pass_non_existing_var(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.subset(var_name = '$t', subset = 'Lat(53.08), Long(8.80), ansi("2014-01":"2014-12")')

# this tests set_format() method
class Test_set_format():
    # pass an existing format(PNG)
    def test_correct_format_png(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.set_format('PNG'), dco)

    # pass an existing format(CSV)
    def test_correct_format_csv(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.set_format('CSV'), dco)
    
    # pass an existing format(JPEG)
    def test_correct_format_jpeg(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.set_format('JPEG'), dco)

    # pass nothing
    def test_pass_nothing(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.set_format()

    # pass non-string argument
    def test_pass_non_string(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.set_format(2)

    # pass non-existing format
    def test_pass_non_existing_format(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.set_format('TIFF')

# this tests where() method
class Test_where():
    # pass an argument with existing var. name and in a string format
    def test_pass_correct(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.where('$c > 2'), dco)

    # pass nothing
    def test_pass_nothing(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.where()
    
    # pass a filter condition with non-existing variable
    def test_pass_non_existing_var(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.where("$t > 10")
    
    # pass non-string argument
    def pass_non_string_arg(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.where(2)

# this tests aggregation functions
class Test_aggregation_functions():
    # this tests correct usage of min() -- condition is passed
    def test_proper_input_min(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.min('$c > 12'), dco)
    
    # this tests correct usage of max() -- condition is passed
    def test_proper_input_max(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.max('$c > 12'), dco)
    
    # this tests correct usage of avg() -- condition is passed
    def test_proper_input_avg(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.avg('$c > 12'), dco)
    
    # this tests correct usage of sum() -- condition is passed
    def test_proper_input_sum(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.sum('$c > 12'), dco)
    
    # this tests correct usage of count() -- condition is passed
    def test_proper_input_count(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.count('$c > 12'), dco)
    
    # this tests correct usage of min() -- no arguments
    def test_proper_empty_min(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.min(), dco)
    
    # this tests correct usage of max() -- no arguments
    def test_proper_empty_max(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.max(), dco)

    # this tests correct usage of avg() -- no arguments
    def test_proper_empty_avg(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.avg(), dco)

    # this tests correct usage of sum() -- no arguments
    def test_proper_empty_sum(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.sum(), dco)

    # this tests correct usage of min() -- no arguments
    def test_proper_empty_count(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.count(), dco)

    # this tests method, when the argument passed is not a string - min()
    def test_non_string_arg_min(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.min(2)

    # this tests method, when the argument passed is not a string - max()
    def test_non_string_arg_max(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.max(2)

    # this tests method, when the argument passed is not a string - avg()
    def test_non_string_arg_avg(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.avg(True)

    # this tests method, when the argument passed is not a string - sum()
    def test_non_string_arg_sum(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.sum(False)

    # this tests method, when the argument passed is not a string - count()
    def test_non_string_arg_count(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.count(2)

    # this tests, when the variable passed does not exist - min()
    def test_var_dont_exist_min(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.min('$t < 10001')

    # this tests, when the variable passed does not exist - max()
    def test_var_dont_exist_max(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.max('$t < 10001')

    # this tests, when the variable passed does not exist - avg()
    def test_var_dont_exist_avg(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.avg('$t < 10001')

    # this tests, when the variable passed does not exist - sum()
    def test_var_dont_exist_sum(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.sum('$t < 10001')

    # this tests, when the variable passed does not exist - count()
    def test_var_dont_exist_count(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.count('$t < 10001')

    # this tests that when multiple aggregation functions are used, only the last one is applied
    # first - no condition, second some condition
    def test_last_applied_1(self):
        my_dco = create_good_dco()
        my_dco.min()
        my_dco.max('$c > 12')
        assert (my_dco.aggregation == 'MAX') and (my_dco.aggregation_condition == '$c > 12')

    # this tests that when multiple aggregation functions are used, only the last one is applied
    # first - some condition, second - no condition
    def test_last_applied_2(self):
        my_dco = create_good_dco()
        my_dco.avg('$c > 12')
        my_dco.sum()
        assert (my_dco.aggregation == 'SUM') and (my_dco.aggregation_condition == None)

# this tests transform_data() method
class Test_transform_data():
    # this tests when the correct transformation operation is passed
    def test_correct_trans_operation(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.transform_data('abs($c - 1000)'), dco)

    # this tests when nothing is passed
    def test_nothing_passed(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.transform_data()

    # this tests when the transformation operation is applied to non existing variable
    def test_non_existing_var(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.transform_data('200 - 100')

    # this tests when the operation passed isn't a string
    def test_non_string_arg(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.transform_data(200)

# this tests encode() function
class Test_encode():
    # nothing is passed
    def test_no_arg(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.encode()

    # non-string operation is passed
    def test_non_string_arg(self):
        my_dco = create_good_dco()
        my_dco.encode(200)
        assert isinstance(my_dco.encode_as, str)

    # this tests when a string passed doesn't have existing variables
    def test_non_exist_vars(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.encode('$t > 12')
    
    # this tests when the correct operation is passed
    def test_correct_format(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.encode('$c > 12'), dco)

# this tests to_wcps_query()
class Test_to_wcps_query():
    # because to_wcps_query() is supposed to be used only by execute() method, we don't expect that non-string argument
    # is passed, because all our methods check that

    # this tests when the correct input is given to the method
    def test_correct_input(self):
        my_dco = create_good_dco()
        my_dco.subset(var_name = '$c', subset = 'ansi("2014-07")')
        my_dco.set_format('PNG')
        assert (my_dco.to_wcps_query() == 'for $c in (AvgLandTemp)\nreturn \nencode($c[ansi("2014-07")] , "image/png")')

    # this tests when the format is not given
    def test_no_format(self):
        my_dco = create_good_dco()
        my_dco.subset(var_name = '$c', subset = 'ansi("2014-07")')
        my_dco.encode(200 + 100)
        assert my_dco.to_wcps_query() == 'for $c in (AvgLandTemp)\nreturn \n300'

    # this tests when transformation is given first and then encoding
    def test_trans_then_encode(self):
        my_dco = create_good_dco()
        my_dco.subset(var_name = '$c', subset = 'ansi("2014-07")')
        my_dco.transform_data('$c + 200')
        my_dco.encode(200 + 100)
        assert my_dco.to_wcps_query() == 'for $c in (AvgLandTemp)\nreturn \n300'
