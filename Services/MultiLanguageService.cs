using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace ExpressBase.ServiceStack.Services
{
    [ClientCanSwapTemplates]
    //[DefaultView("Form")]
    [Authenticate]

    public class MultiLanguageService : EbBaseService
    {
        public MultiLanguageService(IEbConnectionFactory _dbf) : base(_dbf) { }


        //-------------------------------------------------------------------------------------------------------------

        //string[] lines = System.IO.File.ReadAllLines(@"C:\Users\Febin\Downloads\Word-lists-in-csv\Word lists in csv\Aword.csv");
        //StringBuilder query2 = new StringBuilder();
        //query2.Append(@"insert into eb_keys (key) values");
        //int j = 0;
        //for (int i = 1; i < lines.Length; i++)
        //{
        //	if (lines[i - 1] != lines[i])
        //	{
        //		query2.Append("('"+ lines[i].Trim() + "'),");
        //		j++;
        //	}
        //	if (j / 100 == 1)
        //	{
        //		j = 0;
        //		query2.Length = query2.Length - 1;
        //		query2.Append(";");
        //		List<DbParameter> parameters1 = new List<DbParameter>();
        //		var dt1 = this.TenantDbFactory.ObjectsDB.DoNonQuery(query2.ToString(), parameters1.ToArray());
        //		query2.Clear();
        //		query2.Append(@"insert into eb_keys (key) values");
        //		Console.WriteLine("ok   ", +i);
        //	}
        //}

        //string[] lines = System.IO.File.ReadAllLines(@"C:\Users\Febin\Downloads\Word-lists-in-csv\Word lists in csv\Aword.csv");
        //StringBuilder query2 = new StringBuilder();
        //query2.Append(@"insert into eb_keyvalue (key_id,lang_id,value) values");
        //int kidstart = 33;
        //int[] lid = { 1,2,6,7,8,9,10,11,12,13};
        //string[] lnotation = {"ch","sp","hi","en","po","ar","ru","be","pu","ja" };

        //int j = 0;
        //for (int i=1;i< lines.Length; i++)
        //{
        //	if (lines[i - 1] != lines[i])
        //	{
        //		for(int k = 0; k < 10; k++)
        //			query2.Append("(" + kidstart + "," + lid[k] + ",'" + lnotation[k] +lines[i] + "'),");
        //		j++;
        //		kidstart++;
        //	}
        //	if (j / 100 == 1)
        //	{
        //		j = 0;
        //		query2.Length = query2.Length - 1;
        //		query2.Append(";");
        //		List<DbParameter> parameters1 = new List<DbParameter>();
        //		var dt1 = this.TenantDbFactory.ObjectsDB.DoNonQuery(query2.ToString(), parameters1.ToArray());
        //		query2.Clear();
        //		query2.Append(@"insert into eb_keyvalue (key_id,lang_id,value) values");
        //		Console.WriteLine("ok   "+i);
        //	}
        //}


        //-------------------------------------------------------------------------------------------------------------


        public object Get(MLGetSearchResultRqst request)
        {
            Dictionary<int, List<MLSearchResult>> dict = new Dictionary<int, List<MLSearchResult>>();
            string query = string.Format(EbConnectionFactory.ObjectsDB.EB_GET_MLSEARCHRESULT);
            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("KEY", EbDbTypes.String, (request.Key_String + "%")));
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("OFFSET", EbDbTypes.Int32, request.Offset));
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("LIMIT", EbDbTypes.Int32, request.Limit));
            var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(query, parameters.ToArray());
            int i = -1;
            Dictionary<long, int> map = new Dictionary<long, int>();
            var count = ds.Tables[0].Rows[0][0];
            foreach (EbDataRow dr in ds.Tables[1].Rows)
            {
                long k = Convert.ToInt64(dr[0]);
                if (!map.ContainsKey(k))
                {
                    map.Add(k, ++i);
                    dict.Add(i, new List<MLSearchResult>());
                }
                dict[map[k]].Add(new MLSearchResult { KeyId = Convert.ToInt64(dr[0]), Key = dr[1].ToString(), LangId = Convert.ToInt32(dr[2]), Language = dr[3].ToString(), KeyValId = Convert.ToInt64(dr[4]), KeyValue = dr[5].ToString() });
            }
            return new MLGetSearchResultRspns { D_member = dict, Count = Convert.ToInt32(count) };
        }

        public object Get(MLLoadLangRequest request)
        {
            string query = string.Format(@"SELECT id,language FROM eb_languages ORDER BY language ASC");
            List<DbParameter> parameters = new List<DbParameter>();
            var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            Dictionary<string, int> dict = new Dictionary<string, int>();
            foreach (EbDataRow dr in dt.Rows)
                dict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));
            return new MLLoadLangResponse { Data = dict };
        }

        public object Get(MLGetStoredKeyValueRequest request)
        {
            Dictionary<int, MLKeyValue> dict = new Dictionary<int, MLKeyValue>();
            string query = string.Format(@"SELECT C.id, A.key, A.id, B.id, C.value
											FROM eb_keys A, eb_languages B, eb_keyvalue C
											WHERE A.id=C.key_id AND B.id=C.lang_id AND LOWER(A.key) LIKE LOWER(@KEY) 
											ORDER BY B.language ASC");
            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("KEY", EbDbTypes.String, request.Key));
            var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            int i = 0;
            foreach (EbDataRow dr in dt.Rows)
                dict.Add(i++, new MLKeyValue { KeyVal_Id = dr[0].ToString(), Key = dr[1].ToString(), Key_Id = dr[2].ToString(), Lang_Id = dr[3].ToString(), KeyVal_Value = dr[4].ToString() });
            return new MLGetStoredKeyValueResponse { Data = dict };
        }

        public object Get(MLUpdateKeyValueRequest request)
        {
            List<MLKeyValue> list = request.Data;
            if (list.Count() == 0)
                return null;
            int InsertCount = 0;
            StringBuilder query1 = new StringBuilder();
            query1.Append(@"INSERT INTO eb_keyvalue (key_id,lang_id,value) VALUES");
            List<DbParameter> parameters1 = new List<DbParameter>();
            string lid = "@LANG_ID", kval = "@KEY_VALUE", kid = "@KEY_ID";
            int rcount = 0;
            for (int i = 0; i < list.Count(); i++)
                if (list[i].KeyVal_Id == "")
                {
                    query1.Append("( " + (kid + rcount) + "," + (lid + rcount) + "," + (kval + rcount) + "),");
                    parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kid + rcount), EbDbTypes.Int64, list[i].Key_Id));
                    parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((lid + rcount), EbDbTypes.Int32, list[i].Lang_Id));
                    parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kval + rcount), EbDbTypes.String, list[i].KeyVal_Value));
                    rcount++;
                    list.Remove(list[i]);
                    i--;
                    InsertCount++;
                }
            query1.Length = query1.Length - 1;
            query1.Append(";");
            int dt1 = 0;
            if (InsertCount > 0)
                dt1 = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query1.ToString(), parameters1.ToArray());
            if (list.Count() == 0)
                return new MLUpdateKeyValueResponse { Data = dt1 };
            StringBuilder sb = new StringBuilder();
            List<DbParameter> parameters2 = new List<DbParameter>();
            rcount = 0;
            foreach (MLKeyValue obj in list)
            {
                string t = string.Format("UPDATE eb_keyvalue SET value=@{0} WHERE id=@{1};", "keyvalvalue_" + rcount, "keyvalid_" + rcount);
                sb.Append(t);
                parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("keyvalid_" + rcount, EbDbTypes.Int64, obj.KeyVal_Id));
                parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("keyvalvalue_" + rcount, EbDbTypes.String, obj.KeyVal_Value));
                rcount++;
            }
            var dt2 = this.EbConnectionFactory.ObjectsDB.DoNonQuery(sb.ToString(), parameters2.ToArray());
            return new MLUpdateKeyValueResponse { Data = dt1 + dt2 };
        }

        public object Get(MLAddKeyRequest request)
        {
            List<MLAddKey> list = request.Data;

            string query1 = EbConnectionFactory.ObjectsDB.EB_MLADDKEY;
            using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                DbCommand cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, query1);
                cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("KEY", EbDbTypes.String, request.Key));
                var key_id = cmd.ExecuteScalar().ToString();

                StringBuilder query2 = new StringBuilder();
                query2.Append(@"INSERT INTO eb_keyvalue (key_id,lang_id,value) VALUES");
                string kid = "@KEY_ID", lid = "@LANG_ID", kval = "@KEY_VALUE";
                int i = 0;
                List<DbParameter> parameters = new List<DbParameter>();
                foreach (MLAddKey obj in request.Data)
                {
                    query2.Append("(" + (kid + i) + "," + (lid + i) + "," + (kval + i) + "),");
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kid + i), EbDbTypes.Int64, key_id));
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((lid + i), EbDbTypes.Int32, obj.Lang_Id));
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kval + i), EbDbTypes.String, obj.Key_Value));
                    i++;
                }
                query2.Length = query2.Length - 1;
                query2.Append(";");
                var dt = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query2.ToString(), parameters.ToArray());
                return new MLAddKeyResponse { KeyId = Convert.ToInt32(key_id), RowAffected = dt };
            }
        }
    }
}
