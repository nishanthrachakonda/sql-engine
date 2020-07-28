""" Module SQL Parser """
import csv
import sys
import parse

def aggregate_function(a, b, func):
    """Function to implement aggregate functions."""
    return_val = False
    if func == "max":
        a[func] = max(a[func], b)
    elif func == "min":
        a[func] = min(a[func], b)
    elif func == "sum":
        a[func] = a[func]+b
    elif func == "avg":
        a[func][0] = a[func][0]+b
        a[func][1] = a[func][1]+1
    else:
        return_val = True
    return return_val
def where_condition(a, b, oper):
    if oper == "=":
        return a == b
    elif oper == ">":
        return a > b
    elif oper == "<":
        return a < b
    elif oper == ">=":
        return a >= b
    elif oper == "<=":
        return a <= b
def format_col(p1, p2, tmp_elem):
    if p1[0] == -1:
        a = int(p1[1])
    else:
        a = int(tmp_elem[p1[0]][p1[1]])
    if p2[0] == -1:
        b = int(p2[1])
    else:
        b = int(tmp_elem[p2[0]][p2[1]])
    return a, b

class QueryProcess:
    """docstring for QueryProcess."""

    def __init__(self, root):
        self.root = root
        lines = [line.rstrip('\n') for line in open(root+"metadata.txt")]
        table_name, table_num, att_num = "", -1, 0
        self.database_format = {}
        self.column_table = {}
        self.local_memory = {}
        self.query = None
        self.print_object = []
        self.query_object = {"projection": [], "distinct": [],
                             "max": float("-inf"), "min": float("inf"),
                             "sum": float(0), "avg": [float(0), 0],
                             "cond1": [], "cond2": [],
                             "oper": []}
        for line in lines:
            if line in ("<begin_table>", "<end_table>"):
                table_name = ""
                att_num = 0
            elif table_name == "":
                table_name = line
                table_num += 1
            else:
                self.database_format[(table_name, line)] = (table_num, att_num)
                att_num += 1

    def conditions_parser(self, pos):
        column = self.query['conditions'][pos]
        column1 = column[0].split('.')
        chk_error = True
        chk_column = 0
        if column1[0].isdigit():
            chk_error = False
            self.query_object["cond1"].append((-1, int(column1[0])))
        elif column1[0].startswith('-') and column1[0][1:].isdigit():
            chk_error = False
            self.query_object["cond1"].append((-1, int(column1[0])))
        for table in self.query['tables']:
            column1 = column[0].split('.')
            if len(column1) == 1:
                if (table, column[0]) in self.database_format:
                    chk_column += 1
                    chk_error = False
                    self.query_object["cond1"].append(self.database_format[(table, column[0])])
            elif len(column1) == 2 and column1[0] == table:
                if (table, column1[1]) in self.database_format:
                    chk_error = False
                    self.query_object["cond1"].append(self.database_format[(table, column1[1])])
        if chk_column > 1 or chk_error:
            return True
        column2 = column[1].split('.')
        chk_error = True
        chk_column = 0
        if column2[0].isdigit():
            chk_error = False
            self.query_object["cond2"].append((-1, int(column2[0])))
        elif column2[0].startswith('-') and column2[0][1:].isdigit():
            chk_error = False
            self.query_object["cond2"].append((-1, int(column2[0])))
        for table in self.query['tables']:
            column2 = column[1].split('.')
            if len(column2) == 1:
                if (table, column[1]) in self.database_format:
                    chk_column += 1
                    chk_error = False
                    self.query_object["cond2"].append(self.database_format[(table, column[1])])
            elif len(column2) == 2 and column2[0] == table:
                if (table, column2[1]) in self.database_format:
                    chk_error = False
                    self.query_object["cond2"].append(self.database_format[(table, column2[1])])
        if chk_column > 1 or chk_error:
            return True
        return False
    def query_process(self, query_string):
        """Method to process query."""
        parse_error, self.query = parse.query_parser(query_string)
        if parse_error:
            print("sql statement error")
            return
        if self.query["function"] not in (None, "max", "min", "sum", "avg"):
            print("function error")
            return
        for table in self.query['tables']:
            self.local_memory[table] = []
            try:
                with open(self.root+table+'.csv') as csvfile:
                    read_csv = csv.reader(csvfile, delimiter=',')
                    for row in read_csv:
                        self.local_memory[table].append(row)
            except:
                print("table error")
                return
        self.query_object["projection"] = []
        all_flag = False
        if self.query['columns'][0] == '*':
            if len(self.query['columns']) == 1:
                all_flag = True
            else:
                print("sql error")
                return
        if len(self.query["conditions"]) > 0:
            error_chk = self.conditions_parser(0)
            if error_chk:
                print("where condn error")
                return
            self.query_object["oper"].append(self.query["conditions"][0][2])
        if len(self.query["conditions"]) == 3:
            self.query_object["oper"].append(self.query["conditions"][1])
            self.query_object["oper"].append(self.query["conditions"][2][2])
            error_chk = self.conditions_parser(2)
            if error_chk:
                print("where condn error")
                return
        if not all_flag:
            for column in self.query['columns']:
                chk_column = 0
                chk_error = True
                for table in self.query['tables']:
                    columns = column.split('.')
                    if len(columns) == 1:
                        if (table, column) in self.database_format:
                            chk_column += 1
                            chk_error = False
                            self.query_object["projection"].append(self.database_format[(table, column)])
                            self.print_object.append((table, column))
                    elif len(columns) == 2 and columns[0] == table:
                        if (table, columns[1]) in self.database_format:
                            chk_error = False
                            self.print_object.append((table, columns[1]))
                            self.query_object["projection"].append(self.database_format[(table, columns[1])])
                if chk_column > 1 or chk_error:
                    print("columns error")
                    return
        else:
            for table in self.query['tables']:
                for k in self.database_format:
                    if table == k[0]:
                        self.print_object.append((table, k[1]))
                        self.query_object["projection"].append(self.database_format[(table, k[1])])
        for col in self.print_object:
            print(col[0]+'.'+col[1], end=",")
        print()
        self.process_table(0)
        if self.query['function'] is not None:
            if self.query['function'] == "avg":
                tmp = self.query_object[self.query['function']]
                print(tmp[0] / tmp[1])
            else:
                print(self.query_object[self.query['function']])

    def process_table(self, pos, prev_elem=None):
        """Method to process tables."""
        tmp = list(self.local_memory.keys())
        if pos == len(tmp)-1:
            for elem in self.local_memory[tmp[pos]]:
                tmp_elem = []
                if prev_elem is not None:
                    tmp_elem.append(prev_elem)
                tmp_elem.append(elem)
                cond = True
                if len(self.query_object["cond1"]) > 0:
                    o1 = self.query_object["oper"][0]
                    a1, b1 = format_col(self.query_object["cond1"][0],
                                        self.query_object["cond2"][0],
                                        tmp_elem)
                    cond = where_condition(a1, b1, o1)
                if len(self.query_object["cond1"]) == 2:
                    o1 = self.query_object["oper"][2]
                    a1, b1 = format_col(self.query_object["cond1"][1],
                                        self.query_object["cond2"][1],
                                        tmp_elem)
                    if self.query_object["oper"][1] == "and":
                        cond = cond and where_condition(a1, b1, o1)
                    else:
                        cond = cond or where_condition(a1, b1, o1)
                if not cond:
                    continue
                ansi = [tmp_elem[i[0]][i[1]] for i in self.query_object["projection"]]
                if self.query['function'] is None:
                    if self.query['distinct']:
                        if ansi not in self.query_object["distinct"]:
                            for elm in ansi:
                                print(elm, end=",")
                            print()
                            self.query_object["distinct"].append(ansi)
                    else:
                        for elm in ansi:
                            print(elm, end=",")
                        print()
                else:
                    if self.query['distinct']:
                        if ansi not in self.query_object["distinct"]:
                            aggregate_function(self.query_object, float(ansi[0]),
                                               self.query['function'])
                            self.query_object["distinct"].append(ansi)
                    else:
                        aggregate_function(self.query_object, float(ansi[0]),
                                           self.query['function'])
        else:
            for elem in self.local_memory[tmp[pos]]:
                tmp_elem = []
                if pos != 0:
                    tmp_elem.append(prev_elem)
                    tmp_elem.append(elem)
                else:
                    tmp_elem = elem
                self.process_table(pos+1, tmp_elem)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Usage: python engine.py <metadata>")
        sys.exit(1)

    QUERY_PROCESSOR = QueryProcess(sys.argv[1])
    while True:
        command = input("sql> ")
        if command == "quit":
            print("closing.")
            break
        QUERY_PROCESSOR.query_process(command)