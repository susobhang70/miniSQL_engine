#!/usr/bin/python
import csv
import os
import sys
from termcolor import colored
from terminaltables import AsciiTable
from collections import OrderedDict
from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, upcaseTokens

def loadParser():
    # simple demo of using the parsing library to do simple-minded SQL parsing
    # could be extended to include where clauses etc.
    #
    # Copyright (c) 2003,2016, Paul McGuire
    #
    selectStmt = Forward()
    SELECT = Keyword("select", caseless=True)
    FROM = Keyword("from", caseless=True)
    WHERE = Keyword("where", caseless=True)

    ident          = Word( alphas + '*', alphanums + "_$()" ).setName("identifier")
    columnName     = ( delimitedList( ident, ".", combine=True ) ).setName("column name").addParseAction(upcaseTokens)
    columnNameList = Group( delimitedList( columnName ) )
    tableName      = ( delimitedList( ident, ".", combine=True ) ).setName("table name").addParseAction(upcaseTokens)
    tableNameList  = Group( delimitedList( tableName ) )

    whereExpression = Forward()
    and_ = Keyword("and", caseless=True)
    or_ = Keyword("or", caseless=True)
    in_ = Keyword("in", caseless=True)

    E = CaselessLiteral("E")
    binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
    arithSign = Word("+-",exact=1)
    realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                             ( "." + Word(nums) ) ) + 
                Optional( E + Optional(arithSign) + Word(nums) ) )
    intNum = Combine( Optional(arithSign) + Word( nums ) + 
                Optional( E + Optional("+") + Word(nums) ) )

    columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
    whereCondition = Group(
        ( columnRval + binop + columnRval ) |
        ( columnRval + binop + columnName ) |
        ( columnName + binop + columnRval ) |
        ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
        ( columnName + in_ + "(" + selectStmt + ")" ) |
        ( "(" + whereExpression + ")" )
        )
    whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

    selectStmt <<= (SELECT + ('*' | columnNameList)("columns") + 
                    FROM + tableNameList( "tables" ) + 
                    Optional(Group(WHERE + whereExpression), "")("where"))
    global simpleSQL

    simpleSQL = selectStmt
    oracleSqlComment = "--" + restOfLine
    simpleSQL.ignore( oracleSqlComment )

def parseQuery(inputQuery):
    try:
        inputQuery = inputQuery.lower()
        tokens = simpleSQL.parseString( inputQuery )
        columns = tokens.columns
        tables = tokens.tables
        where = tokens.where
        # print columns
        # print columns, tables, where
        return columns, tables, where
    except ParseException, err:
        print colored("[ERROR]", 'red'), err

def getFiles(path):
    if not os.path.isdir(path):
        print colored("[ERROR]",'red'),"Invalid path: Path does not exist... ", path
        new_path = raw_input("Please enter new path (or leave blank for current path): ")
        if not new_path:
            new_path = os.path.dirname(os.path.abspath(__file__))
        new_path, files = getFiles(new_path)
        return new_path, files
    else:
        files = os.listdir(path)
        print colored("[INFO]",'green')+" Found %s files... Loading databases" % len(files)
        return path, files

def group(seq, sep):
    # http://stackoverflow.com/questions/15357830/python-spliting-a-list-based-on-a-delimiter-word
    g = []
    for el in seq:
        if el == sep:
            yield g
            g = []
        g.append(el)
    yield g

def loadDatabases(path, files):
    if "metadata.txt" not in files:
        print colored("[ERROR]",'red'),"Metadata not found"
        return "error"

    else:
        try:
            with open(path + '/metadata.txt','r') as metadata:
                content = metadata.read().splitlines()
                # print content
                tables = list(group(content, "<begin_table>"))[1:]
                # print tables
                tableSchema = OrderedDict()
                # global list_table_names = []
                for table in tables:
                    # print table
                    tableName = table[1].lower()
                    # list_table_names.append(tableName)
                    tableSchema[tableName] = OrderedDict()
                    for col in table[2:-1]:
                        # print col
                        tableSchema[tableName][col] = []
            # print tableSchema

            new_filelist = []
            for file in files:
                if file.lower().endswith('.csv'):
                    new_filelist.append(file)
            # print new_filelist
            for file in new_filelist:
                with open(path + '/' + file, 'r') as table:
                    tableName = file.split('.')[0].lower()
                    data = [row for row in csv.reader(table, delimiter=',', skipinitialspace=True)]
                    # print data
                    if not data:
                        print colored("[INFO]",'red'),tableName, " database is empty."
                        contents = []
                    else:
                        contents = data[0:]
                    for row in contents:
                        # print row
                        it = 0
                        for col in tableSchema[tableName]:
                            try:
                                # print tableName
                                tableSchema[tableName][col].append(int(row[it]))
                            except:
                                print colored("[ERROR]",'red'),"Cannot read, make sure value is integral. Storing NULL"
                                tableSchema[tableName][col].append("NULL")
                            it += 1
            # print tableSchema
            return tableSchema

        except:
            print colored("[ERROR]",'red'),"Metadata file doesn't match database entries"
            return "error"

def checkTables(columns, tables):
    for table in tables:
        if table.lower() not in databases:
            print colored("[ERROR]", 'red')+ " Table %s doesn't exist in database" % table
            return False, table

    colTableList = []
    for col in columns:
        if col.find('.') == -1 and col.find('*') == -1 and col.find('(') == -1:
            frequencyCol = 0
            for table in tables:
                if col in databases[table.lower()]:
                    frequencyCol += 1
                    colTableList.append([table, col])
                if frequencyCol > 1:
                    print colored("[ERROR]", 'red')+ " Ambiguous column query %s" % col
                    return False, table
            if frequencyCol == 0:
                print colored("[ERROR]", 'red')+ " Column %s not found in specified table(s)" % col
                return False, table

        elif col.find('*') != -1:
            # print "yoo2"
            for table in tables:
                for header in databases[table.lower()]:
                    colTableList.append([table, header])

        elif col.find('.') != -1 and col.find('(') == -1:
            colTable = col[:col.find('.')]
            colName = col[col.find('.') + 1 : ]
            # print "yoo"
            flag = 0
            for table in tables:
                if colTable.lower() == table.lower():
                    if colName in databases[table.lower()]:
                        colTableList.append([table, colName])
                        flag = 1
                    break
            if not flag:
                print colored("[ERROR]", 'red')+ " Column %s not found in specified table(s)" % col
                return False, table

        elif col.find('(') != -1:
            if col.find(')') == -1:
                print colored("[ERROR]", 'red')+ " Column %s : syntax error" % col
                return False, table
            index = col.find('(')
            endindex = col.find(')')
            function = col[:index]
            if col.find('.') == -1:
                colName = col[index + 1 : endindex]
                frequencyCol = 0
                for table in tables:
                    if colName in databases[table.lower()]:
                        frequencyCol += 1
                        colTableList.append([table, colName, function])
                    if frequencyCol > 1:
                        print colored("[ERROR]", 'red')+ " Ambiguous column query %s" % col
                        return False, table
                if frequencyCol == 0:
                    print colored("[ERROR]", 'red')+ " Column %s not found in specified table(s)" % col
                    return False, table
            else:
                colTable = col[index + 1 : col.find('.')]
                colName = col[col.find('.') + 1 : endindex]
                flag = 0
                for table in tables:
                    if colTable.lower() == table.lower():
                        if colName in databases[table.lower()]:
                            colTableList.append([table, colName, function])
                            flag = 1
                        break
                if not flag:
                    print colored("[ERROR]", 'red')+ " Column %s not found in specified table(s)" % col
                    return False, table

        else:
            print colored("[ERROR]", 'red') + "Invalid query"
            return False, table

    return True, colTableList

def checkConditions(where, tables):
    conditions = []
    conjunction = ""
    queryConditions = where[0]
    if(len(queryConditions) > 4):
        print colored("[ERROR]", 'red') + " Too many conditions"
        return
    for i in range(len(queryConditions)):
        if i == 0:
            continue
        elif i == 2:
            conjunction = queryConditions[2]
        else:
            if len(queryConditions[i]) > 3:
                print colored("[ERROR]", 'red')+ " Syntax error in where clause"
                return
            temp = []
            for j in range(len(queryConditions[i])):
                indexDot = queryConditions[i][j].find('.')
                if j == 1:
                    continue
                elif indexDot == -1:
                    if queryConditions[i][j].isdigit() or queryConditions[i][j][:1] == '-':
                        temp.append([queryConditions[i][j]])
                    else:
                        col = queryConditions[i][j]
                        frequencyCol = 0
                        for table in tables:
                            if col in databases[table.lower()]:
                                frequencyCol += 1
                                temp.append([table, col])
                            if frequencyCol > 1:
                                print colored("[ERROR]", 'red')+ " Ambiguous column query %s after where" % col
                                return
                        if frequencyCol == 0:
                            print colored("[ERROR]", 'red')+ " Column %s not found in specified table(s)" % col
                            return
                elif indexDot != -1:
                    colTable = queryConditions[i][j][:indexDot]
                    colName = queryConditions[i][j][indexDot + 1: ]
                    flag = 0
                    for table in tables:
                        if colTable.lower() == table.lower():
                            if colName in databases[table.lower()]:
                                temp.append([table, colName])
                                flag = 1
                            break
                    if not flag:
                        print colored("[ERROR]", 'red')+ " Column %s not found in specified table(s)" % queryConditions[i][j]
                        return
                if j == 2:
                    conditions.append(temp)
    return conditions, conjunction

def recursiveJoinProduct(joinProduct, resultTable, level, templists):
    if level == 1:
        for i in resultTable[len(resultTable) - level]:
            templists.extend(i)
            temp = templists[:]
            joinProduct.append(temp)
            for j in i:
                templists.pop()
        return joinProduct, templists

    for i in resultTable[len(resultTable) - level]:
        templists.extend(i)
        joinProduct, templists = recursiveJoinProduct(joinProduct, resultTable, level - 1, templists)
        for j in i:
            templists.pop()
    return joinProduct, templists

def printTable(queryList, resultArr, origQueryList, conditions):
    tableSet = set()
    for entry in queryList:
        tableSet.add(entry[0])
    finalrows = []
    for i in tableSet:
        finalrows.append([i, set(range(0,1000))])
    finalrows.sort()

    for entry, colList in zip(queryList, resultArr):
        tableSet = set([m[1] for m in colList])
        for j in range(len(finalrows)):
            if finalrows[j][0] == entry[0]:
                finalrows[j][1] = finalrows[j][1] & tableSet

    for entry in finalrows:
        entry[1] = list(entry[1])
        entry[1].sort()

    resultTable = []
    i = -1
    for entry in finalrows:
        i += 1
        curcol = entry[1]
        resultTable.append([])
        for j in curcol:
            resultTable[i].append([])

    # print resultTable
    noConditionFlag = True
    for i in range(len(conditions)):
        if len(conditions[i][0]) == 2 and len(conditions[i][1]) == 2:
            noConditionFlag = False
            break

    i = -1
    for entry in finalrows:
        i += 1
        for queryEntry, resultEntry in zip(queryList, resultArr):
            if queryEntry[0] == entry[0]:
                temporaryList = resultEntry
                for tempEntry in temporaryList:
                    if tempEntry[1] in entry[1]:
                        resultTable[i][entry[1].index(tempEntry[1])].append(tempEntry[0])

    joinProduct = []
    header = []
    for entry in origQueryList:
        if len(entry) == 3:
            tempheader = entry[2] + '(' + entry[0] + '.' + entry[1] + ')'
            header.append(tempheader)
        else:
            header.append(entry[0] + '.' + entry[1])
    joinProduct.append(header)

    if noConditionFlag:
        joinProduct, templists = recursiveJoinProduct(joinProduct, resultTable, len(resultTable), [])
    else:
        for i in resultTable[0]:
            joinProduct.append([])
        for i in resultTable:
            for j in range(len(i)):
                for k in i[j]:
                    joinProduct[j+1].append(k)

    if len(conditions) > 0:
        if len(conditions) > 1:
            if len(conditions[0][0]) == 2 and len(conditions[0][1]) == 2:
                if len(conditions[1][0]) == 2 and len(conditions[1][1]) == 2:
                    columnNames = []
                    for i in conditions[0]:
                        for j in conditions[1]:
                            if i == j:
                                colName = str(i[0]) + '.' + str(i[1])
                                columnNames.append(colName)
                    try:
                        for columnName in columnNames:
                            tempIndex = joinProduct[0].index(columnName)
                            for row in joinProduct:
                                del row[tempIndex]
                    except:
                        pass
                else:
                    colName = str(conditions[0][1][0]) + '.' + str(conditions[0][1][1])
                    firstcolName = str(conditions[0][0][0]) + '.' + str(conditions[0][0][1])
                    try:
                        if conditions[0][0] in queryList:
                            tempIndex = joinProduct[0].index(colName)
                            for row in joinProduct:
                                del row[tempIndex]

                        elif conditions[0][1] in queryList and conditions[0][0] not in queryList:
                            tempIndex = joinProduct[0].index(firstcolName)
                            for row in joinProduct:
                                del row[tempIndex]
                    except:
                        pass
            elif len(conditions[1][0]) == 2 and len(conditions[1][1]) == 2:
                colName = str(conditions[1][1][0]) + '.' + str(conditions[1][1][1])
                firstcolName = str(conditions[1][0][0]) + '.' + str(conditions[1][0][1])
                try:
                    if conditions[1][0] in queryList:
                        tempIndex = joinProduct[0].index(colName)
                        for row in joinProduct:
                            del row[tempIndex]

                    elif conditions[1][1] in queryList and conditions[1][0] not in queryList:
                        tempIndex = joinProduct[0].index(firstcolName)
                        for row in joinProduct:
                            del row[tempIndex]
                except:
                    pass
        elif len(conditions) == 1:
            if len(conditions[0][0]) == 2 and len(conditions[0][1]) == 2:
                colName = str(conditions[0][1][0]) + '.' + str(conditions[0][1][1])
                firstcolName = str(conditions[0][0][0]) + '.' + str(conditions[0][0][1])
                try:
                    if conditions[0][0] in queryList:
                        tempIndex = joinProduct[0].index(colName)
                        for row in joinProduct:
                            del row[tempIndex]

                    elif conditions[0][1] in queryList and conditions[0][0] not in queryList:
                        tempIndex = joinProduct[0].index(firstcolName)
                        for row in joinProduct:
                            del row[tempIndex]
                except:
                    pass

        newJoinProduct = []
        for i in range(len(joinProduct)):
            if len(joinProduct[i]) < len(joinProduct[0]):
                pass
            else:
                newJoinProduct.append(joinProduct[i])
        joinProduct = newJoinProduct[:]

    table = []
    if len(joinProduct) != 1:
        table = AsciiTable(joinProduct)
        print table.table
    else:
        print
    return table

def solveCondition(conditions, index):
    resultArr = []
    if conditions[index][0][0] == conditions[index][1][0]:
        # print "this"
        temp = []
        table = conditions[index][0][0]
        firstCol  = conditions[index][0][1]
        secondCol = conditions[index][1][1]
        for l in range(len(databases[table.lower()][firstCol])):
            if databases[table.lower()][firstCol][l] == databases[table.lower()][secondCol][l]:
                temp.append(l)
        resultArr.append([conditions[index][0][0], temp])

    elif len(conditions[index][1]) == 1:
        temp = []
        table = conditions[index][0][0]
        tempcol = conditions[index][0][1]
        for l in range(len(databases[table.lower()][tempcol])):
            if int(databases[table.lower()][tempcol][l]) == int(conditions[index][1][0]):
                temp.append(l)
        resultArr.append([conditions[index][0][0], temp])

    else:
        temp1 = []
        temp2 = []
        firstTable  = conditions[index][0][0]
        secondTable = conditions[index][1][0]
        firstCol  = conditions[index][0][1]
        secondCol = conditions[index][1][1]
        for m in range(len(databases[firstTable.lower()][firstCol])):
            for n in range(len(databases[secondTable.lower()][secondCol])):
                if databases[firstTable.lower()][firstCol][m] == databases[secondTable.lower()][secondCol][n]:
                    temp1.append(m)
                    temp2.append(n)
        resultArr.append([firstTable, temp1])
        resultArr.append([secondTable, temp2])
    return resultArr

def solveWithConditions(querylist, conditions, conjunction):
    rowList = []
    firstCond = []
    secondCond = []
    for i in range(len(conditions)):
        for j in range(len(conditions[i])):
            if j == 0 and len(conditions[i][j]) == 1:
                print colored("[ERROR]", 'red') + "Equate column to integer, not vice versa"
                return []
    firstCond = solveCondition(conditions, 0)
    if len(conditions) == 2:
        secondCond = solveCondition(conditions, 1)

    if conjunction:
        for statement in firstCond:
            for secondStatement in secondCond:
                if statement[0] == secondStatement[0]:
                    firstSet = set(statement[1])
                    secondSet = set(secondStatement[1])
                    temp = {}
                    if conjunction.lower() == 'and':
                        temp = firstSet & secondSet
                    else:
                        temp = firstSet | secondSet
                    templist = list(temp)
                    templist.sort()
                    rowList.append([statement[0], templist])

    for statement in firstCond:
        flag = 0
        for row in rowList:
            if statement[0] == row[0]:
                flag = 1
                break
        if flag == 0:
            rowList.append(statement)

    for statement in secondCond:
        flag = 0
        for row in rowList:
            if statement[0] == row[0]:
                flag = 1
                break
        if flag == 0:
            rowList.append(statement)

    return rowList

def solveWithoutConditions(querylist, rowList = []):
    # print "hello"
    resultArr = []
    for entry in querylist:
        temparr = []
        tableName = entry[0]
        colName = entry[1]
        if(len(entry) == 3):
            if entry[2] == 'MAX' or entry[2] == 'max':
                tempmax = -sys.maxint
                index = -1
                if rowList:
                    tabflag = 0
                    for row in rowList:
                        if row[0] == tableName:
                            tabflag = 1
                            tempCols = row[1]
                            for l in range(len(databases[tableName.lower()][colName])):
                                if int(databases[tableName.lower()][colName][l]) > tempmax and l in tempCols:
                                    tempmax = int(databases[tableName.lower()][colName][l])
                            if tempmax != -sys.maxint:
                                temparr.append([str(tempmax),0])
                    if tabflag == 0:
                        for l in databases[tableName.lower()][colName]:
                            if int(l) > tempmax:
                                tempmax = int(l)
                        if tempmax != -sys.maxint:
                            temparr.append([str(tempmax),0])
                else:
                    try:
                        for l in databases[tableName.lower()][colName]:
                            if int(l) > tempmax:
                                tempmax = int(l)
                    except:
                        print colored("[ERROR]", 'red') + " Column %s contains non-integer values" % colName
                        return
                    if tempmax != -sys.maxint:
                        temparr.append([str(tempmax), 0])

            elif entry[2] == 'MIN' or entry[2] == 'min':
                tempmin = sys.maxint
                if rowList:
                    tabflag = 0
                    for row in rowList:
                        if row[0] == tableName:
                            tabflag = 1
                            tempCols = row[1]
                            for l in range(len(databases[tableName.lower()][colName])):
                                if int(databases[tableName.lower()][colName][l]) < tempmin and l in tempCols:
                                    tempmin = int(databases[tableName.lower()][colName][l])
                            if tempmin != -sys.maxint:
                                temparr.append([str(tempmin),0])
                    if tabflag == 0:
                        for l in databases[tableName.lower()][colName]:
                            if int(l) < tempmin:
                                tempmin = int(l)
                        if tempmin != sys.maxint:
                            temparr.append([str(tempmin),0])
                else:
                    try:
                        for l in databases[tableName.lower()][colName]:
                            if int(l) < tempmin:
                                tempmin = int(l)
                    except:
                        print colored("[ERROR]", 'red') + " Column %s contains non-integer values" % colName
                        return
                    if tempmin != sys.maxint:
                        temparr.append([str(tempmin), 0])

            elif entry[2] == 'SUM' or entry[2] == 'sum':
                tempmin = sys.maxint
                sum = 0
                if rowList:
                    tabflag = 0
                    for row in rowList:
                        if row[0] == tableName:
                            tabflag = 1
                            tempCols = row[1]
                            for l in range(len(databases[tableName.lower()][colName])):
                                if l in tempCols:
                                    sum += int(databases[tableName.lower()][colName][l])
                            temparr.append([str(sum),0])
                    if tabflag == 0:
                        for l in databases[tableName.lower()][colName]:
                            sum += int(l)
                        temparr.append([str(sum),0])
                else:
                    for l in databases[tableName.lower()][colName]:
                        sum += int(l)
                    temparr.append([str(sum), 0])

            elif entry[2] == 'AVG' or entry[2] == 'avg' or entry[2] == 'average':
                sum = 0
                count = 0
                if rowList:
                    tabflag = 0
                    for row in rowList:
                        sum1 = 0
                        count1 = 0
                        if row[0] == tableName:
                            tabflag = 1
                            tempCols = row[1]
                            for l in range(len(databases[tableName.lower()][colName])):
                                if l in tempCols:
                                    sum1 += int(databases[tableName.lower()][colName][l])
                                    count1 += 1
                            avg = sum1/float(count1)
                            temparr.append([str(avg1), 0])

                    if tabflag == 0:
                        for l in databases[tableName.lower()][colName]:
                            sum += int(l)
                            count += 1
                        avg = sum/float(count)    
                        temparr.append([str(avg),0])
                else:
                    for l in databases[tableName.lower()][colName]:
                        sum += int(l)
                    count = len(databases[tableName.lower()][colName])
                    avg = sum / float(count)
                    temparr.append([str(avg), 0])

            elif entry[2] == 'DISTINCT' or entry[2] == 'distinct':
                duplicate = []
                newtemparr = [[x, databases[tableName.lower()][colName].index(x)]\
                                for x in databases[tableName.lower()][colName] if x not in duplicate and (duplicate.append(x) or True)]
                if rowList:
                    tabflag = 0
                    for row in rowList:
                        if row[0] == tableName:
                            tabflag = 1
                            tempCols = row[1]
                            for y in range(len(newtemparr)):
                                if newtemparr[y][1] in tempCols:
                                    temparr.append([newtemparr[y][0], newtemparr[y][1]])
                    if tabflag == 0:
                        temparr = newtemparr[:]
                else:
                    temparr = newtemparr[:]
        else:
            if rowList:
                tabflag = 0
                for row in rowList:
                    if tableName == row[0]:
                        tabflag = 1
                        tempCols = row[1]
                        for l in range(len(databases[tableName.lower()][colName])):
                            if l in tempCols:
                                temparr.append([databases[tableName.lower()][colName][l], l])
                if tabflag == 0:
                    for l in range(len(databases[tableName.lower()][colName])):
                        temparr.append([databases[tableName.lower()][colName][l], l])
                # pass
            else:
                for l in range(len(databases[tableName.lower()][colName])):
                    temparr.append([databases[tableName.lower()][colName][l], l])
        resultArr.append(temparr)
    # print resultArr
    return querylist, resultArr

def executeQuery(query):
    try:
        columns, tables, where = parseQuery(query)
        bValidTable, tableQueryList = checkTables(columns, tables)
        origQueryList = tableQueryList[:]
        if not bValidTable:
            return
        conditions, conjunction = checkConditions(where, tables)
        # print tableQueryList, conditions, conjunction
        if not conditions:
            tableQueryList, result = solveWithoutConditions(tableQueryList)
            # print result
        else:
            rowList = solveWithConditions(tableQueryList, conditions, conjunction)
            tableQueryList, result = solveWithoutConditions(tableQueryList, rowList)
        printTable(tableQueryList, result, origQueryList, conditions)
    except:
        print colored("[ERROR]", 'red') + " Oops, error - please retry"
        return

def queryEngine():
    while 1:
        query = raw_input(colored("SqlEngine> ", 'white'))
        print query
        if query == "quit" or query == "q":
            return
        if not query:
            continue
        executeQuery(query)
        # print query

def main():
    loadParser()
    global databases
    if len(sys.argv) > 1:
        cur_path = os.path.dirname(os.path.abspath(__file__))
        path, files = getFiles(cur_path)
        databases = loadDatabases(path, files)
        if databases == "error":
            print colored("[ERROR]", 'red'), "Error loading databases, please fix and retry"
            return
        executeQuery(sys.argv[1])
    else:
        path = raw_input("Enter the path to database files (or press enter for current path): ")
        if( not path ):
            path = os.path.dirname(os.path.abspath(__file__))
        newpath, files = getFiles(path)
        databases = loadDatabases(newpath, files)
        if databases == "error":
            print colored("[ERROR]", 'red'), "Error loading databases, please fix and retry"
            return
        queryEngine()

if __name__ == "__main__":
    main()