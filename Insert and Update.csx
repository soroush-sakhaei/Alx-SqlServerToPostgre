#r "$NuGet\ApplicationExtensions\1.1.9\lib\net45\ApplicationExtensions.dll"
using Application.IEnumerableExtensions;
using Application.IDictionaryExtensions;
using Application.ObjectExtensions;

var Data = new[] {
    new {
        PersonId = 1,
        Name = "John Smith"
    },
    new {
        PersonId = 2,
        Name = "Soroush Sakhaei"
    }
};

// The command returns a List of SQL insert commands.  This can be useful for batch
//PostgreSQL.InsertRowsCommand("person", Data, 1000).Dump();


// If you want all the InsertCommands together, then you can call the command this way
//PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue).First().Dump();

// The purpose of MultiLineNum is to determine how many insert rows to combine into one row.  This is for performance and 25 is a good default number in SQL Server.
//PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25).First().Dump();

// Sometimes a database will generate primary key columns when a row is added.  By specifying ColumnsToReturn, you will get SQL that will when ran return the values of those primary keys
PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"}).First().Dump();

// Sometimes you want to manually specify primary keys and sometimes you want a database to specify them for you.  In SQL Server, you have to turn Identity insert on to specify a primary key in a table that by default generates keys
//PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"}, false).First().Dump();

// The InserRowsCommand is interesting because it lets you pass any type of object that "looks like" your destination table.
// The main key is that the property names have to match the column names.  Sometimes your objects have columns that you don't want to be included in the insertion 
//PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"}, false, new[] {"Name"}).First().Dump();

// This would almost never be used in SQL Server.  I'm not sure if PostgreSQL has this issue, but there are a few rare data types that cannot be quoted in SQL.  This lets you specify which ones not to quote.
// By default it's usually safe to quote all data
//PostgreSQL.InsertRowsCommand("Person", Data, int.MaxValue, 25, new[] {"PersonId"}, false, new[] {"Name"}, new HashSet<string> {"Name"}).First().Dump();

// The update command is a lot more straight forward.  It updates the Data in the database using the specified primary key columns to find the right data to update
//PostgreSQL.UpdateRowsCommand("person", new [] {"PersonId"}, Data).Dump();

// This update command works by updating rows that found from a query.  This example will update anyone with a first name of Mohammad to have the last name of Smith.
//PostgreSQL.UpdateRowsCommand("Person", new[] { PostgreSQL.SetWhereRow.Create(new {
//        LastName = "Smith"
//    }, new {
//        FirstName = "Mohammad"
//    })
//}).Dump();



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
            var stringBuilder = indentityInsert ? new StringBuilder("SET IDENTITY_INSERT ").Append(tableName).Append(" ON").Append(Environment.NewLine) : new StringBuilder();
            //var OutPutCommand = columnsToReturn != null ? (new StringBuilder("output")).Append(columnsToReturn.Aggregate(x => (new StringBuilder("inserted.")).Append(x).Append(""),
            //    (result, x) => result.Append(", ").Append("inserted.").Append(x).Append(""))).Append(" into @Result ") : null;
            //if (columnsToReturn != null)
             //   stringBuilder.Append("DECLARE @Result TABLE (").Append(columnsToReturn.Aggregate(x => (new StringBuilder("")).Append(x).Append("").Append(" nvarchar(max)"), (result, x) => result.Append(", ")
             //           .Append(x).Append(" nvarchar(max)"))).Append(");\r\n");

            if (columnsToExclude != null)
            {
                var ExcludeSet = columnsToExclude.ToHashSet();
                data = data.Select(x => x.Where(y => !ExcludeSet.Contains(y.Key)).ToDictionary(y => y.Key, y => y.Value));
            }
            var qmark=Convert.ToChar(34);
            var ColumnCMD = data.First().Select(x => new StringBuilder("\"").Append(x.Key).Append(qmark)).Aggregate(x => new StringBuilder("(").Append(x), (result, x) => result.Append(", ").Append(x));
            
            foreach (var rows in data.ChunkifyToList((list, y) => list.Count != multiLineNum))
            {
                stringBuilder.Append("INSERT INTO ");
                stringBuilder.Append(tableName);
                stringBuilder.Append(" ");
                stringBuilder.Append(ColumnCMD);
                stringBuilder.Append(") ");
               // if (columnsToReturn != null)
                //    stringBuilder.Append(OutPutCommand);
                stringBuilder.Append(" VALUES\r\n");

                stringBuilder.Append(rows.Select(r => r.Select(x => PostgreSQL.GetSQLValueBuilder(x.Value, unquotedColumns.Contains(x.Key))).Aggregate(x => new StringBuilder("(").Append(x),
                    (result, x) => result.Append(", ").Append(x)).Append(")"))
                    .Aggregate(x => x, (result, x) => result.Append(",\r\n").Append(x)));
                stringBuilder.Append(" returning ");
                var qmark2=Convert.ToChar(34);
                var ColumnCMD2 = data.First().Select(x => new StringBuilder("\"").Append(x.Key).Append(qmark2)).Aggregate(x => new StringBuilder("").Append(""), (result, x) => result.Append("").Append(x));
                stringBuilder.Append(ColumnCMD2);
                stringBuilder.Append(";\r\n ");
                
            }
            //if (columnsToReturn != null)
             //   stringBuilder.Append(";SELECT * FROM ").Append(tableName).Append(";");
            if (indentityInsert)
                stringBuilder.Append("SET IDENTITY_INSERT ").Append(tableName).Append(" OFF;");
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
                builder.Append("UPDATE ").Append(tableName).Append(" SET ").Append(column.Where(x => !primaryKeyColumnNames.Contains(x.Key))
                                        .Select(x => new StringBuilder("").Append(x.Key).Append(" ").Append(GetSQLSetValueBuilder(x.Value)))
                                        .Aggregate(x => x,
                                            (result, x) => result.Append(", ").Append(x)));
var qmark=Convert.ToChar(34);
                if (primaryKeyColumnNames.Any())
                    builder.Append(" WHERE ").Append((primaryKeyColumnNames.Select(x => qmark + x +qmark+ " = '" + column[x] + "'")
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
                builder.Append("UPDATE ").Append(tableName).Append(" SET ").Append(diff.Item1
                                        .Select(x => new StringBuilder("").Append(x.Key).Append(" ").Append(GetSQLSetValueBuilder(x.Value)))
                                        .Aggregate(x => x,
                                            (result, x) => result.Append(", ").Append(x)));
                if (diff.Item2.Any())
                    builder.Append(" WHERE ").Append(diff.Item2.Select(x => new StringBuilder("" + x.Key + " ")
                                .Append(useLike ? GetSQLLikeValueBuilder(x.Value) : GetSQLEqualsValueBuilder(x.Value)))
                            .Aggregate(x => x, (result, x) => result.Append(" AND ").Append(x)));

                builder.Append(";\r\n");
            }

            return builder.Length > 0 ? builder.ToString() : ";";
        }
}



public class SetWhereRow<T, S>
{
    public T SetData { get; set; }

    public S WhereData { get; set; }
}