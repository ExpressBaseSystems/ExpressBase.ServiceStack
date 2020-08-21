using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class ApiConversionService : EbBaseService
    {
        private ResponseStatus _Responsestatus = new ResponseStatus();

        public ApiConversionResponse Any(ApiConversionRequest request)
        {
            ApiConversionResponse resp = null;
            resp = start_get(request);
            return resp;
        }

        public HttpResponseMessage Execute(ApiConversionRequest request)
        {
            List<Param> param = (request.Parameters != null) ? request.Parameters :new List<Param>();
            string Url = request.Url;
            if (request.Method == ApiMethods.GET && param.Count >0)
                Url = ModifyUrl(Url, param);
            var uri = new Uri(Url);
            HttpResponseMessage response = null;
            try
            {
                using (var client = new HttpClient())
                {
                    if (request.Headers != null && request.Headers.Any())
                    {
                        foreach (ApiRequestHeader header in request.Headers)
                        {
                            client.DefaultRequestHeaders.Add(header.HeaderName, header.Value);
                        }
                    }

                    client.BaseAddress = new Uri(uri.GetLeftPart(System.UriPartial.Authority));
                    if (request.Method == ApiMethods.POST)
                    {
                        var parameters = param.Select(i => new { prop = i.Name, val = i.Value })
                                .ToDictionary(x => x.prop, x => x.val);
                        response = client.PostAsync(uri.PathAndQuery, new FormUrlEncodedContent(parameters)).Result;
                    }
                    else if (request.Method == ApiMethods.GET)
                    {
                        response = client.GetAsync(uri.PathAndQuery).Result;
                    }
                }
            }
            catch(Exception e)
            {
                throw e;
            }
            return response;
        }

        private string ModifyUrl(string longurl, List<Param> param)
        {
            var uriBuilder = new UriBuilder(longurl);
            var query = HttpUtility.ParseQueryString(uriBuilder.Query);
            foreach(Param i in param)
                query[i.Name] = i.Value;
            //query["action"] = "login1";
            uriBuilder.Query = query.ToString();
            longurl = uriBuilder.ToString();
            return longurl;
        }
        
        private List<Param> GetParams(List<ApiRequestParam>  Parameters)
        {
            return Parameters.Select(i => new Param { Name = i.ParamName, Type = i.Type.ToString(), Value = i.Value })
                    .ToList();
        }

        private ApiConversionResponse start_get(ApiConversionRequest request)
        {
            ApiConversionResponse resp = new ApiConversionResponse();
            HttpResponseMessage response = null;
            EbDataSet ds = new EbDataSet();
            try
            {
                response = Execute(request);
                var jsonObject = JObject.Parse(response.Content.ReadAsStringAsync().Result);
                int i = -1;
                foreach (var item in jsonObject)
                {
                    if (item.Value.Type == JTokenType.Array)
                    {
                        i++;int k = 0;
                        ds.Tables.Add(new EbDataTable());
                        foreach (var children in item.Value.Children())
                        {
                            foreach (var prop in children.Children<JProperty>().ToArray())
                            {
                                ds.Tables[i].Columns.Add(new EbDataColumn { ColumnIndex = k++, ColumnName = prop.Name, Type = ConvertToEbdbType(prop.Value.Type) });
                            }
                            break;
                        }
                        int j = 0;
                        foreach (var children in item.Value.Children())
                        {
                            ds.Tables[i].Rows.Add(ds.Tables[i].NewDataRow2());
                            k = 0;
                            foreach (var prop in children.Children<JProperty>().ToArray())
                            {
                                ds.Tables[i].Rows[j][k++] = ConvertValueToEbdbType( prop.Value.Type, prop.Value);
                            }
                            j++;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Message);
                this._Responsestatus.Message = e.Message;
            }
            resp.dataset = ds;
            if(response != null)
                resp.statusCode = (int)response.StatusCode;
            return resp;
        }

        public EbDbTypes ConvertToEbdbType(JTokenType _type)
        {
            if (_type == JTokenType.Integer)
                return EbDbTypes.Int32;
            else if (_type == JTokenType.Boolean)
                return EbDbTypes.Boolean;
            else if (_type == JTokenType.Float)
                return EbDbTypes.Decimal;
            else if (_type == JTokenType.Date)
                return EbDbTypes.DateTime;
            else 
                return EbDbTypes.String;
        }

        public object ConvertValueToEbdbType(JTokenType _type, JToken value)
        {
            if (_type == JTokenType.Integer)
                return Convert.ToInt32(value);
            else if (_type == JTokenType.Boolean)
                return Convert.ToBoolean(value);
            else if (_type == JTokenType.Float)
                return Convert.ToDecimal(value);
            else if (_type == JTokenType.Date)
                return Convert.ToDateTime(value);
            else
                return value.ToString();
        }

        //public string ApiErrorHandling(int )
        //{

        //}
    }
}
