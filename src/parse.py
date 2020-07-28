""" Module SQL Parser """
import sqlparse


def attribute_parser(attribute_string):
    """Function to parse columns and table."""
    list_attribute = attribute_string.replace(" ", "").split(",")
    return list_attribute

def conditions_parser(condition_string):
    """Function to parse where condition."""
    list_condition = []
    for token in condition_string:
        if token.ttype is sqlparse.tokens.Keyword and token.value != "where":
            list_condition.append(token.value)
        elif isinstance(token, sqlparse.sql.Comparison):
            # print(token.left, token.right, token.token_next(0)[1])
            list_condition.append((token.left.value, token.right.value,
                                   token.token_next(0)[1].value))
    return list_condition

def query_parser(query_string):
    """Function to parse a given query."""
    # statements = sqlparse.split(arg)
    # for query in statements:
    query = sqlparse.format(query_string, keyword_case="lower")
    parser, = sqlparse.parse(query)
    format_query = {"distinct": False, "function": None, "columns": [],
                    "tables": [], "conditions": []}
    is_table = False
    # print(parser.tokens)
    if sqlparse.sql.Identifier(parser.tokens).is_wildcard():
        format_query["columns"] = attribute_parser("*")
    if parser.tokens[0].ttype is not sqlparse.tokens.Keyword.DML:
        # print("yes")
        return True, None
    for token in parser.tokens:
        if isinstance(token, sqlparse.sql.Where):
            # print(sqlparse.sql.Where(token))
            format_query["conditions"] = conditions_parser(sqlparse.sql.Where(token))
            if not format_query["conditions"]:
                return True, None
        elif token.ttype is sqlparse.tokens.Keyword:
            if token.value == "distinct":
                format_query["distinct"] = True
            elif token.value == "from":
                is_table = True
        elif isinstance(token, (sqlparse.sql.IdentifierList, sqlparse.sql.Identifier)):
            if is_table:
                format_query["tables"] = attribute_parser(token.value)
            else:
                format_query["columns"] = attribute_parser(token.value)
        elif isinstance(token, sqlparse.sql.Function):
            format_query['function'] = token.get_name().lower()
            param = list(token.get_parameters())
            if len(param) != 1:
                return True, None
            format_query["columns"] = attribute_parser(param[0].value)

    if not format_query["columns"] or not format_query["tables"]:
        return True, None

    return False, format_query

if __name__ == '__main__':
    TEST_STR = 'select max(a, b) from table,table1 where x=10=1;'
    # t, = sqlparse.parse(TEST_STR)
    # print(t.get_type())
    # if t.ttype is sqlparse.tokens.Keyword.DML:
    #     print("yes")
    #     # good_stmnts.append(stmnt)
    print(query_parser(TEST_STR))
