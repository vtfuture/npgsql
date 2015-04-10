using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Npgsql;

using NpgsqlTests.DataModels;

using NUnit.Framework;

namespace NpgsqlTests
{
    [TestFixture]
    public class CopyTests : TestBase
    {
        public CopyTests(string backendVersion)
            : base(backendVersion)
        {
        }

        [Test]
        public void StringIsInsertedCorrectly()
        {
            var fullStr = TestUtil.ReadResource("data.command_output.json");
            var model = JsonConvert.DeserializeObject<CommandModel>(fullStr);
            
            var cmd = Conn.CreateCommand();
            cmd.CommandText = "COPY data(field_pk, field_text) FROM STDIN;";

            var serializer = new NpgsqlCopySerializer(Conn);
            var copyIn = new NpgsqlCopyIn(cmd, Conn, serializer.ToStream);
            copyIn.Start();

            serializer.AddInt32(1);
            serializer.AddString(fullStr);
            serializer.EndRow();

            serializer.AddInt32(2);
            serializer.AddString(model.Output);
            serializer.EndRow();

            serializer.AddInt32(3);
            serializer.AddString(model.OutputData);
            serializer.EndRow();

            serializer.AddInt32(4);
            serializer.AddString("abc");
            serializer.EndRow();

            serializer.AddInt32(5);
            serializer.AddString("ab");
            serializer.EndRow();

            serializer.AddInt32(6);
            serializer.AddString("a");
            serializer.EndRow();

            serializer.Flush();
            serializer.Close();

            copyIn.End();

            var readFullStrCmd = Conn.CreateCommand();
            readFullStrCmd.CommandText = "SELECT field_text FROM data where field_pk = 1";
            var fullStrFromDb = (string)readFullStrCmd.ExecuteScalar();
            Assert.AreEqual(fullStr, fullStrFromDb);

            var readOutputStrCmd = Conn.CreateCommand();
            readOutputStrCmd.CommandText = "SELECT field_text FROM data where field_pk = 2";
            var outputStrFromDb = (string)readOutputStrCmd.ExecuteScalar();
            Assert.AreEqual(model.Output, outputStrFromDb);

            var readOutputDataStrCmd = Conn.CreateCommand();
            readOutputDataStrCmd.CommandText = "SELECT field_text FROM data where field_pk = 3";
            var outputDataStrFromDb = (string)readOutputDataStrCmd.ExecuteScalar();
            
            Assert.AreEqual(model.OutputData, outputDataStrFromDb);

            var readSimpleStringUneven = Conn.CreateCommand();
            readSimpleStringUneven.CommandText = "SELECT field_text FROM data where field_pk = 4";
            var outputSimpleStringUneven = (string)readSimpleStringUneven.ExecuteScalar();

            Assert.AreEqual("abc", outputSimpleStringUneven);

            var readSimpleStringEven = Conn.CreateCommand();
            readSimpleStringEven.CommandText = "SELECT field_text FROM data where field_pk = 5";
            var outputSimpleStringEven = (string)readSimpleStringEven.ExecuteScalar();

            Assert.AreEqual("ab", outputSimpleStringEven);

            var readSimpleStringOneChar = Conn.CreateCommand();
            readSimpleStringOneChar.CommandText = "SELECT field_text FROM data where field_pk = 6";
            var outputSimpleStringOneChar = (string)readSimpleStringOneChar.ExecuteScalar();

            Assert.AreEqual("a", outputSimpleStringOneChar);
        }
    }
}
