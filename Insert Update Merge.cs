#r "$NuGet\ApplicationExtensions\1.1.9\lib\net45\ApplicationExtensions.dll"
using Application.IEnumerableExtensions;
using Application.IDictionaryExtensions;
using Application.ObjectExtensions;

var Data = new[] {
    new {
        PersonId = 1,
        FirstName = "John Smith"
    },
    new {
        PersonId = 2,
        FirstName = "Soroush Sakhaei"
    },
    new {
        PersonId = 3,
        FirstName = "Alex Williams"
    },
    new {
        PersonId = 4,
        FirstName = "Ali Karimi"
    }
};

// The command returns a List of SQL insert commands.  This can be useful for batch
PostgreSQL.InsertRowsCommand("Person", Data, 1000, 25,null,true).Dump();


// If you want all the InsertCommands together, then you can call the command this way
PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue,25,null,true).First().Dump();

// The purpose of MultiLineNum is to determine how many insert rows to combine into one row.  This is for performance and 25 is a good default number in SQL Server.
PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25,null,true).First().Dump();

// Sometimes a database will generate primary key columns when a row is added.  By specifying ColumnsToReturn, you will get SQL that will when ran return the values of those primary keys
PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"},true).First().Dump();

// Sometimes you want to manually specify primary keys and sometimes you want a database to specify them for you.  In SQL Server, you have to turn Identity insert on to specify a primary key in a table that by default generates keys
PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"}, true).First().Dump();

// The InserRowsCommand is interesting because it lets you pass any type of object that "looks like" your destination table.
// The main key is that the property names have to match the column names.  Sometimes your objects have columns that you don't want to be included in the insertion 
PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"}, true, new[] {"Name"}).First().Dump();

// This would almost never be used in SQL Server.  I'm not sure if PostgreSQL has this issue, but there are a few rare data types that cannot be quoted in SQL.  This lets you specify which ones not to quote.
// By default it's usually safe to quote all data
PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"}, true, new[] {"Name"}, new HashSet<string> {"Name"}).First().Dump();

// The update command is a lot more straight forward.  It updates the Data in the database using the specified primary key columns to find the right data to update
PostgreSQL.UpdateRowsCommand("Person", new [] {"PersonId"}, Data).Dump();

PostgreSQL.MergeRowsCommand("Person",new[] {"PersonId"},Data,true,25).Dump();


public static class PostgreSQL
{
    public static class SetWhereRow
    {
        public static SetWhereRow<T, S> Create<T, S>(T setData, S whereData)
        {
            return new SetWhereRow<T, S> { SetData = setData, WhereData = whereData };
        }
    }

    public static StringBuilder DefaultBuilder = new StringBuilder("DEFAULT");

    public static StringBuilder IsNULLBuilder = new StringBuilder("IS NULL");

    public static StringBuilder EqualsNullBuilder = new StringBuilder("= NULL");

    public static StringBuilder NULLBuilder = new StringBuilder("NULL");

    public static StringBuilder GetSQLEqualsValueBuilder(string value)
    {
        return value != null ? new StringBuilder("= '").Append(value.Replace("'", "''")).Append("'") : IsNULLBuilder;
    }

    public static StringBuilder GetSQLLikeValueBuilder(string value)
    {
        return value != null ? new StringBuilder("LIKE '").Append(value.Replace("'", "''")).Append("'") : IsNULLBuilder;
    }

    public static StringBuilder GetSQLSetValueBuilder(string value)
    {
        return value != null ? new StringBuilder("= '").Append(value.Replace("'", "''")).Append("'") : EqualsNullBuilder;
    }

    public static StringBuilder GetSQLValueBuilder(string value, bool omitQuote)
    {
        if (omitQuote)
            return value != null ? new StringBuilder(value) : PostgreSQL.DefaultBuilder;
        else
            return value != null ? new StringBuilder("'").Append(value.Replace("'", "''")).Append("'") : PostgreSQL.DefaultBuilder;
    }

    public static IEnumerable<string> InsertRowsCommand(string tableName, object data, int batchRowCount, int multiLineNum = 25, IEnumerable<string> columnsToReturn = null, bool indentityInsert = false, IEnumerable<string> columnsToExclude = null, HashSet<string> unquotedColumns = null)
    {
        var Data = data as IEnumerable<object> ?? new[] { data };

        return Data.ChunkifyToList((list, y) => list.Count != batchRowCount).Select(x =>
            InsertRowsCommand(tableName, x.Select(y => y.ToStringStringDictionary()), multiLineNum, columnsToReturn, indentityInsert, columnsToExclude, unquotedColumns));
    }



    private static string InsertRowsCommand(string tableName, IEnumerable<IDictionary<string, string>> data, int multiLineNum, IEnumerable<string> columnsToReturn = null, bool indentityInsert = false, IEnumerable<string> columnsToExclude = null, HashSet<string> unquotedColumns = null)
    {
        if (!data.Any())
            return ";";

        tableName = tableName[0] == '[' ? tableName : "" + tableName + "";
        unquotedColumns = unquotedColumns ?? new HashSet<string>();

        //var ColumnCMD2 = data.First().Select(x => new StringBuilder("\"")).Aggregate(x => new StringBuilder("").Append(x), (result, x) => result.Append(", ").Append(x));

        var stringBuilder = new StringBuilder("");
        string tableNameUpper = null;
        var qmark = Convert.ToChar(34);
        if (tableName.Any(char.IsUpper))
        {
            tableNameUpper = tableName;
            tableName = tableName.ToLower();
            stringBuilder.Append("Alter Table ").Append(qmark).Append(tableNameUpper).Append(qmark).Append(" Rename To ").Append(tableName).Append(";").Append(System.Environment.NewLine);
        }

        if (columnsToExclude != null)
        {
            var ExcludeSet = columnsToExclude.ToHashSet();
            data = data.Select(x => x.Where(y => !ExcludeSet.Contains(y.Key)).ToDictionary(y => y.Key, y => y.Value));
        }
        //var qmark=Convert.ToChar(34);
        var ColumnCMD = data.First().Select(x => new StringBuilder("\"").Append(x.Key).Append("\"")).Aggregate(x => new StringBuilder("(").Append(x), (result, x) => result.Append(", ").Append(x));
        string colCMD = ColumnCMD.ToString();
        int c1 = colCMD.IndexOf(",") + 3;
        string IdVal = colCMD.Substring(c1, colCMD.Length - c1 - 1);
        //stringBuilder.Append(IdVal);

        //stringBuilder.Append(ColumnCMD);



        if (indentityInsert)
        {
            stringBuilder.Append("Alter Table ").Append(tableName).Append(System.Environment.NewLine);
            stringBuilder.Append("Alter Column ").Append(qmark).Append(IdVal).Append(qmark).Append(System.Environment.NewLine);
            stringBuilder.Append("Drop Identity If Exists;").Append(System.Environment.NewLine);
        }

        foreach (var rows in data.ChunkifyToList((list, y) => list.Count != multiLineNum))
        {
            stringBuilder.Append("INSERT INTO ");
            stringBuilder.Append(tableName);
            stringBuilder.Append(" ");
            stringBuilder.Append(ColumnCMD);
            stringBuilder.Append(") ");
            stringBuilder.Append(" VALUES\r\n");
            stringBuilder.Append(rows.Select(r => r.Select(x => PostgreSQL.GetSQLValueBuilder(x.Value, unquotedColumns.Contains(x.Key))).Aggregate(x => new StringBuilder("(").Append(x),
                (result, x) => result.Append(", ").Append(x)).Append(")"))
                .Aggregate(x => x, (result, x) => result.Append(",\r\n").Append(x)));
            if (columnsToReturn != null)
            {
                stringBuilder.Append(" returning ");
                var qmark2 = Convert.ToChar(34);
                var ColumnCMD2 = data.First().Select(x => new StringBuilder("\"").Append(x.Key).Append(qmark2)).Aggregate(x => new StringBuilder("").Append(""), (result, x) => result.Append("").Append(x));
                stringBuilder.Append(ColumnCMD2);
            }

            stringBuilder.Append(";\r\n ");

        }

        if (indentityInsert)
        {

            stringBuilder.Append("Alter Table ").Append(tableName).Append(System.Environment.NewLine);
            stringBuilder.Append("Alter Column ").Append(qmark).Append(IdVal).Append(qmark).Append(System.Environment.NewLine);
            stringBuilder.Append("Add Generated Always As Identity;").Append(System.Environment.NewLine);
            stringBuilder.Append("select setval(pg_get_serial_sequence('").Append(tableName).Append("','").Append(IdVal).Append("'), (select max(").Append(qmark).Append(IdVal).Append(qmark).Append(") from ").Append(tableName).Append("));").Append(System.Environment.NewLine);
        }

        if (tableNameUpper != null)
        {

            stringBuilder.Append("Alter Table ").Append(tableName).Append(" Rename To ").Append(qmark).Append(tableNameUpper).Append(qmark).Append(";").Append(System.Environment.NewLine);
        }



        return stringBuilder.Length > 0 ? stringBuilder.ToString() : ";";
    }



    public static string UpdateRowsCommand(string tableName, IEnumerable<string> primaryKeyColumnNames, object data)
    {
        var Data = data as IEnumerable<object> ?? new[] { data };
        return UpdateRowsCommand(tableName, primaryKeyColumnNames, Data.Select(x => x.ToStringStringDictionary()));
    }

    private static string UpdateRowsCommand(string tableName, IEnumerable<string> primaryKeyColumnNames, IEnumerable<IDictionary<string, string>> columns)
    {
        tableName = tableName[0] == '[' ? tableName : "" + tableName + "";
        var builder = new StringBuilder();
        foreach (var column in columns)
        {
            builder.Append("UPDATE ").Append("\"").Append(tableName).Append("\"").Append(" SET ").Append("\"").Append(column.Where(x => !primaryKeyColumnNames.Contains(x.Key))
                                    .Select(x => new StringBuilder("").Append(x.Key).Append("\"").Append(GetSQLSetValueBuilder(x.Value)))
                                    .Aggregate(x => x,
                                        (result, x) => result.Append(", ").Append(x)));
            var qmark = Convert.ToChar(34);
            if (primaryKeyColumnNames.Any())
                builder.Append(" WHERE ").Append((primaryKeyColumnNames.Select(x => "\"" + x + "\"" + " = '" + column[x] + "'")
                        .Aggregate(x => x, (result, x) => result + " AND " + x)));

            builder.Append(";\r\n");
        }

        return builder.Length > 0 ? builder.ToString() : ";";
    }

    public static string UpdateRowsCommand<T, S>(string tableName, IEnumerable<SetWhereRow<T, S>> setWhereDiffs, bool useLike = false)
    {
        return UpdateRowsCommand(tableName, setWhereDiffs.Select(x => Tuple.Create(x.SetData.ToStringStringDictionary(), x.WhereData.ToStringStringDictionary())), useLike);
    }

    private static string UpdateRowsCommand(string tableName, IEnumerable<Tuple<IDictionary<string, string>, IDictionary<string, string>>> setWhereDiffs, bool useLike = false)
    {
        tableName = tableName[0] == '[' ? tableName : "" + tableName + "";
        var builder = new StringBuilder();
        foreach (var diff in setWhereDiffs)
        {
            builder.Append("UPDATE ").Append("\"").Append(tableName).Append("\"").Append(" SET ").Append("\"").Append(diff.Item1
                                    .Select(x => new StringBuilder("").Append(x.Key).Append("\"").Append(GetSQLSetValueBuilder(x.Value)))
                                    .Aggregate(x => x,
                                        (result, x) => result.Append(", ").Append(x)));
            if (diff.Item2.Any())
                builder.Append(" WHERE ").Append("\"").Append(diff.Item2.Select(x => new StringBuilder(x.Key).Append("\"")
                            .Append(useLike ? GetSQLLikeValueBuilder(x.Value) : GetSQLEqualsValueBuilder(x.Value)))
                        .Aggregate(x => x, (result, x) => result.Append(" AND ").Append(x)));

            builder.Append(";\r\n");
        }

        return builder.Length > 0 ? builder.ToString() : ";";
    }
    
    
    

    public static string DeleteIfExistsCommand(string tableName)
    {
        return "DROP TABLE IF EXISTS " + "\"" + tableName + "\"" + ";";
    }

    public static List<string> MergeRowsCommand(string tableName, IEnumerable<string> primaryKeyColumnNames, IEnumerable<object> data, bool tableHasIdentity, int batchRowCount, string setCMD = null, bool deleteUnspecifiedRows = false, Dictionary<string, string> ColumnsToDefault = null, IEnumerable<string> naturalKeyColumns = null, HashSet<string> unquotedColumns = null)
    {
        var qmark = Convert.ToChar(34);
        var dictData = data.Select(x => x.ToStringStringDictionary());
        var Result = new List<string>();
        var builder = new StringBuilder("");
        string tableNameUpper = null;
        if (tableName.Any(char.IsUpper))
        {
            tableNameUpper = tableName;
            tableName = tableName.ToLower();
            builder.Append("Alter Table ").Append(qmark).Append(tableNameUpper).Append(qmark).Append(" Rename To ").Append(tableName).Append(";").Append(System.Environment.NewLine);
        }


        var TempTableName = tableName + "temp";
        var columnNames = dictData.Select(x => x.Keys).First();

        var columnNamesDelimited = columnNames.Select(x => qmark + x + qmark).Aggregate(x => new StringBuilder(x), (result, x) => result.Append(", ").Append(x));


        var tempId = "";
        foreach (string value in primaryKeyColumnNames)
        {
            tempId = value.ToString();
        }
        var IdDef = tempId;

        var setColumnsCommand = columnNames.Where(x => !primaryKeyColumnNames.Contains(x)).Select(x => "" + x + "")
                                    .Aggregate(x => new StringBuilder("").Append(qmark).Append(x).Append(qmark).Append(" = ").Append(qmark).Append(TempTableName).Append(qmark).Append(".").Append(qmark).Append(x).Append(qmark),
                                        (result, x) => result.Append(", ").Append(tableName).Append(".").Append(x).Append(" = ").Append(TempTableName).Append(".").Append(x));

        builder.Append(DeleteIfExistsCommand(TempTableName)).Append(System.Environment.NewLine).Append("SELECT ").Append(columnNamesDelimited).Append(" INTO ").Append(qmark).Append(TempTableName).Append(qmark).Append(" from ").Append(qmark).Append(tableName)
            .Append(qmark).Append(@" fetch first 0 rows only; ").Append(System.Environment.NewLine);



        tableHasIdentity = tableHasIdentity && naturalKeyColumns == null;
        if (tableHasIdentity)
        {
            builder.Append("Alter Table ").Append(TempTableName).Append(System.Environment.NewLine);
            builder.Append("Alter Column ").Append(qmark).Append(IdDef).Append(qmark).Append(System.Environment.NewLine);
            builder.Append("Drop Identity If Exists;");

        }


        if (ColumnsToDefault != null)
            builder.Append(ColumnsToDefault.Aggregate("", (a, c) => a + string.Format("Alter table {0} add constraint def_temp_{0}_{1} default " + c.Value + " for {1};", TempTableName, c.Key))).Append(System.Environment.NewLine);
        Result.Add(builder.Length > 0 ? builder.ToString() : ";");
        dictData.ChunkifyToList((list, y) => list.Count != batchRowCount).ToList().ForEach(y =>
        {
            Result.Add(InsertRowsCommand(TempTableName, y, 25, null, false, null, unquotedColumns));
        });
        builder = new StringBuilder("");

        if (setCMD != null)
            builder.Append("UPDATE ").Append(qmark).Append(TempTableName).Append(qmark).Append(" ").Append(setCMD).Append(";");
        naturalKeyColumns = naturalKeyColumns ?? primaryKeyColumnNames;
        if (deleteUnspecifiedRows && !naturalKeyColumns.IsNullOrEmpty())
            builder.Append("DELETE a FROM " + tableName + " a LEFT OUTER JOIN " + qmark + TempTableName + qmark +
                " b ON ").Append(naturalKeyColumns.Select(x => "a." + x + " = b." + x + "")
                            .Aggregate(x => x, (result, x) => result + " AND " + x))
                        .Append(" WHERE ")
                        .Append(naturalKeyColumns.Select(x => "b." + x + " IS NULL")
                            .Aggregate(x => x, (result, x) => result + " OR " + x)).Append(";");
        if (!naturalKeyColumns.IsNullOrEmpty())
            builder.Append("UPDATE ").Append(tableName).Append(" SET ").Append(setColumnsCommand).Append(" FROM ").Append(qmark).Append(TempTableName).Append(qmark).Append(" where ")
                .Append((naturalKeyColumns.Select(x => "" + qmark + TempTableName + qmark + "." + qmark + x + qmark + " = " + qmark + tableName + qmark + "." + qmark + x + qmark)
                            .Aggregate(x => x, (result, x) => result + " AND " + x) + ";")).Append(System.Environment.NewLine);


        var Data = data as IEnumerable<object> ?? new[] { data };
        //var IdCMD = data.First().Select(x => new StringBuilder("\"").Append(x.Key).Append(qmark)).Aggregate(x => new StringBuilder("(").Append(x), (result, x) => result.Append(", ").Append(x));                   
        if (tableHasIdentity)
        {
            builder.Append("Alter Table ").Append(tableName).Append(System.Environment.NewLine);
            builder.Append("Alter Column ").Append(qmark).Append(IdDef).Append(qmark).Append(System.Environment.NewLine);
            builder.Append("Drop Identity If Exists;");
        }

        builder.Append(System.Environment.NewLine).Append(@"INSERT INTO ")
               .Append(tableName).Append(@" (").Append(columnNamesDelimited).Append(") SELECT ")
               .Append(columnNames.Select(x => "A." + qmark + x + qmark).Aggregate(x => new StringBuilder(x), (result, x) => result.Append(", ").Append(x)))
               .Append(" FROM ").Append(TempTableName);
        if (!naturalKeyColumns.IsNullOrEmpty())
            builder.Append(@" AS A
						LEFT JOIN ").Append(tableName).Append(@" AS B ON ")
                        .Append(naturalKeyColumns.Select(x => @"A." + qmark + x + qmark + @" = B." + qmark + x + qmark + @"")
                                        .Aggregate(x => x, (result, x) => result + " AND " + x))
                        .Append(" WHERE ").Append(naturalKeyColumns.Select(x => "B." + qmark + x + qmark + " IS NULL;").Aggregate(x => x, (result, x) => result + " OR " + x));
        if (tableHasIdentity)
        {

            builder.Append(System.Environment.NewLine).Append("Alter Table ").Append(tableName).Append(System.Environment.NewLine);
            builder.Append("Alter Column ").Append(qmark).Append(IdDef).Append(qmark).Append(System.Environment.NewLine);
            builder.Append("Add Generated Always As Identity;");
            builder.Append(System.Environment.NewLine).Append("select setval(pg_get_serial_sequence('").Append(tableName).Append("','").Append(IdDef).Append("'), (select max(").Append(qmark).Append(IdDef).Append(qmark).Append(") from ").Append(tableName).Append("));").Append(System.Environment.NewLine);

        }

        if (tableNameUpper != null)
        {

            builder.Append("Alter Table ").Append(tableName).Append(" Rename To ").Append(qmark).Append(tableNameUpper).Append(qmark).Append(";").Append(System.Environment.NewLine);
        }

        builder.Append("DROP TABLE IF EXISTS ").Append(qmark).Append(TempTableName).Append(qmark).Append(";");
        Result.Add(builder.Length > 0 ? builder.ToString() : ";");
        return Result;
    }
}


public class SetWhereRow<T, S>
{
    public T SetData { get; set; }

    public S WhereData { get; set; }
}
