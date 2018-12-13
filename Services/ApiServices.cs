using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ApiServices : EbBaseService
    {
        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public FormDataJsonResponse Post(FormDataJsonRequest request)
        {
            WebFormSchema schema = JsonConvert.DeserializeObject<WebFormSchema>(request.JsonData);

            EbSqlFunction func = new EbSqlFunction(schema,this.EbConnectionFactory);

            return new FormDataJsonResponse { };
        }

        //generate insert obj and update object
        private void GenJsonColumns(WebformData data)
        {
            FormSqlData sqlData = new FormSqlData();

            foreach (KeyValuePair<string, SingleTable> kp in data.MultipleTables)
            {
                List<JsonColVal> insertcols = new List<JsonColVal>();
                List<JsonColVal> updatecols = new List<JsonColVal>();

                foreach (SingleRow _row in kp.Value)
                {
                    JsonColVal jsoncols_ins = new JsonColVal();
                    JsonColVal jsoncols_upd = new JsonColVal();

                    if (_row.IsUpdate)
                        updatecols.Add(this.GetCols(jsoncols_upd, _row));
                    else
                        insertcols.Add(this.GetCols(jsoncols_ins, _row));
                }
                if (insertcols.Count > 0)
                {
                    sqlData.JsonColoumsInsert.Add(new JsonTable
                    {
                        TableName = kp.Key,
                        Rows = insertcols
                    });
                }
                if (updatecols.Count > 0)
                {
                    sqlData.JsonColoumsUpdate.Add(new JsonTable
                    {
                        TableName = kp.Key,
                        Rows = updatecols
                    });
                }
            }
        }

        private JsonColVal GetCols(JsonColVal col, SingleRow row)
        {
            foreach (SingleColumn _cols in row.Columns)
            {
                col.Add(_cols.Name, _cols.Value);
            }
            return col;
        }
    }
}
