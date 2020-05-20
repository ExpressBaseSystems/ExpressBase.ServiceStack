using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class FileOperationServices : EbBaseService
    {
        public FileOperationServices(IEbConnectionFactory _dbf, IMessageProducer _msp, IMessageQueueClient _mqc) : base(_dbf, _msp, _mqc)
        {
        }

        [Authenticate]
        public FileCategoryChangeResponse Post(FileCategoryChangeRequest request)
        {
            int result;
            //string sql = EbConnectionFactory.DataDB.EB_FILECATEGORYCHANGE;
            try
            {
                Console.WriteLine("Cat: " + request.Category);
                Console.WriteLine("Ids: " + request.FileRefId.Join(","));

                string slectquery = EbConnectionFactory.DataDB.EB_FILECATEGORYCHANGE;
                DbParameter[] parameters =
                {
                    this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, request.FileRefId.Join(",")),
                };

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(slectquery, parameters);

                StringBuilder dystring = new StringBuilder();

                foreach (EbDataRow row in dt.Rows)
                {
                    int id = Convert.ToInt32(row["id"]);

                    EbFileMeta meta;
                    try
                    {
                        meta = JsonConvert.DeserializeObject<EbFileMeta>(row["tags"].ToString());
                        if (meta == null)
                            meta = new EbFileMeta();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        meta = new EbFileMeta();
                    }

                    meta.Category.Clear();
                    meta.Category.Add(request.Category);
                    string serialized = JsonConvert.SerializeObject(meta);
                    dystring.Append(string.Format("UPDATE eb_files_ref SET tags='{0}' WHERE id={1};", serialized, id));
                }

                result = this.EbConnectionFactory.DataDB.DoNonQuery(dystring.ToString());
            }
            catch (Exception ex)
            {
                result = 0;
                Console.BackgroundColor = ConsoleColor.Red;
                Console.WriteLine("Exception while updating Category:" + ex.Message);
            }

            return new FileCategoryChangeResponse { Status = (result > 0) ? true : false };
        }
    }
}

