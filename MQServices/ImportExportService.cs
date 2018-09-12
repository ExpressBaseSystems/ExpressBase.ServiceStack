﻿using ExpressBase.Common;
using ExpressBase.Common.Objects;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    [Authenticate]
    public class ImportrExportService : EbMqBaseService
    {
        public ImportrExportService(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec)
        {
        }

        public ExportApplicationResponse Post(ExportApplicationMqRequest request)
        {
            ExportApplicationResponse resp = new ExportApplicationResponse();
            MessageProducer3.Publish(new ExportApplicationRequest
            {
                AppId = request.AppId,
                Refids = request.Refids,
                BToken = this.ServerEventClient.BearerToken,
                RToken = this.ServerEventClient.RefreshToken,
                SolnId = request.SolnId,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                WhichConsole = request.WhichConsole
            });

            return resp;
        }
        public ImportApplicationResponse Get(ImportApplicationMqRequest request)
        {
            ImportApplicationResponse resp = new ImportApplicationResponse();
            this.MessageProducer3.Publish(new ImportApplicationRequest
            {
                Id = request.Id,
                BToken = this.ServerEventClient.BearerToken,
                RToken = this.ServerEventClient.RefreshToken,
                SolnId = request.SolnId,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                WhichConsole = request.WhichConsole
            });
            return resp;
        }
    }

    [Restrict(InternalOnly = true)]
    public class ImportExportInternalService : EbMqBaseService
    {
        public ImportExportInternalService(IServiceClient _ssclient) : base(_ssclient) { }

        public string Post(ExportApplicationRequest request)
        {
            OrderedDictionary ObjDictionary = new OrderedDictionary();
            try
            {
                ServiceStackClient.RefreshToken = request.RToken;
                ServiceStackClient.BearerToken = request.BToken;
                AppWrapper AppObj = ServiceStackClient.Get(new GetApplicationRequest { Id = request.AppId }).AppInfo;
                AppObj.ObjCollection = new List<EbObject>();
                string[] refs = request.Refids.Split(",");
                foreach (string _refid in refs)
                    GetRelated(_refid, ObjDictionary);

                ICollection ObjectList = ObjDictionary.Values;
                foreach (object item in ObjectList)
                    AppObj.ObjCollection.Add(item as EbObject);
                SaveToAppStoreResponse p = ServiceStackClient.Post(new SaveToAppStoreRequest
                {
                    Store = new AppStore
                    {
                        Name = AppObj.Name,
                        Cost = 10.00m,
                        Currency = "USD",
                        Json = EbSerializers.Json_Serialize(AppObj),
                        Status = 1,
                        AppType = 1,
                        Description = AppObj.Description,
                        Icon = AppObj.Icon
                    },
                    SolnId = request.SolnId,
                    UserId = request.UserId,
                    UserAuthId = request.UserAuthId,
                    WhichConsole = request.WhichConsole
                });
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            return null;
        }

        public string Post(ImportApplicationRequest request)
        {
            Dictionary<string, string> RefidMap = new Dictionary<string, string>();
            try
            {
                ServiceStackClient.RefreshToken = request.RToken;
                ServiceStackClient.BearerToken = request.BToken;
                AppWrapper AppObj = ServiceStackClient.Get(new GetOneFromAppStoreRequest
                {
                    Id = request.Id,
                    SolnId = request.SolnId,
                    UserAuthId = request.UserAuthId,
                    UserId = request.UserId,
                    WhichConsole = request.WhichConsole,
                }).Wrapper;
                List<EbObject> ObjectCollection = AppObj.ObjCollection;
                UniqueApplicationNameCheckResponse uniq_appnameresp;

                do
                {
                    uniq_appnameresp = ServiceStackClient.Get<UniqueApplicationNameCheckResponse>(new UniqueApplicationNameCheckRequest { AppName = AppObj.Name });
                    if (!uniq_appnameresp.IsUnique)
                        AppObj.Name = AppObj.Name + "(1)";
                }
                while (!uniq_appnameresp.IsUnique);
                CreateApplicationResponse appres = ServiceStackClient.Post(new CreateApplicationDevRequest
                {
                    AppName = AppObj.Name,
                    AppType = AppObj.AppType,
                    Description = AppObj.Description,
                    AppIcon = AppObj.Icon
                });

                for (int i = ObjectCollection.Count - 1; i >= 0; i--)
                {
                    UniqueObjectNameCheckResponse uniqnameresp;
                    EbObject obj = ObjectCollection[i];

                    do
                    {
                        uniqnameresp = ServiceStackClient.Get(new UniqueObjectNameCheckRequest { ObjName = obj.Name });
                        if (!uniqnameresp.IsUnique)
                            obj.Name = obj.Name + "(1)";
                    }
                    while (!uniqnameresp.IsUnique);

                    obj.ReplaceRefid(RefidMap);
                    EbObject_Create_New_ObjectRequest ds = new EbObject_Create_New_ObjectRequest
                    {
                        Name = obj.Name,
                        Description = obj.Description,
                        Json = EbSerializers.Json_Serialize(obj),
                        Status = ObjectLifeCycleStatus.Dev,
                        Relations = "_rel_obj",
                        IsSave = false,
                        Tags = "_tags",
                        Apps = appres.id.ToString(),
                        SourceSolutionId = (obj.RefId.Split("-"))[0],
                        SourceObjId = (obj.RefId.Split("-"))[3],
                        SourceVerID = (obj.RefId.Split("-"))[4],
                        SolnId = request.SolnId,
                        UserId = request.UserId,
                        UserAuthId = request.UserAuthId,
                        WhichConsole = request.WhichConsole
                    };
                    EbObject_Create_New_ObjectResponse res = ServiceStackClient.Post(ds);
                    RefidMap[obj.RefId] = res.RefId;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return null;
        }
        public void GetRelated(string _refid, OrderedDictionary ObjDictionary)
        {
            EbObject obj = null;

            if (ObjDictionary.Contains(_refid))
            {
                obj = (EbObject)ObjDictionary[_refid];
                ObjDictionary.Remove(_refid);
            }
            else
                obj = GetObjfromDB(_refid);

            ObjDictionary.Add(_refid, obj);
            string RefidS = obj.DiscoverRelatedRefids();

            string[] _refCollection = RefidS.Split(",");
            foreach (string _ref in _refCollection)
                if (_ref.Trim() != string.Empty)
                    GetRelated(_ref, ObjDictionary);
        }
        public EbObject GetObjfromDB(string _refid)
        {
            // EbObjectService ObjectService = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse res = ServiceStackClient.Get(new EbObjectParticularVersionRequest { RefId = _refid });
            EbObject obj = EbSerializers.Json_Deserialize(res.Data[0].Json);
            obj.RefId = _refid;
            return obj;
        }
    }
}
