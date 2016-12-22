using MimeKit;
using ProtoBuf;
using ServiceStack;
using ServiceStack.Web;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class ProtoBufServiceClient : ServiceClientBase
    {
        public override string Format
        {
            get { return "x-protobuf"; }
        }

        public ProtoBufServiceClient(string baseUri)
        {
            SetBaseUri(baseUri);
        }

        public ProtoBufServiceClient(string syncReplyBaseUri, string asyncOneWayBaseUri)
            : base(syncReplyBaseUri, asyncOneWayBaseUri) { }

        public override void SerializeToStream(IRequest req, object request, Stream stream)
        {
            Serializer.NonGeneric.Serialize(stream, request);
        }

        public override T DeserializeFromStream<T>(Stream stream)
        {
            return Serializer.Deserialize<T>(stream);
        }

        public override string ContentType
        {
            get { return "x-protobuf"; }
        }

        public override StreamDeserializerDelegate StreamDeserializer
        {
            get { return Deserialize; }
        }

        private static object Deserialize(Type type, Stream source)
        {
            return Serializer.NonGeneric.Deserialize(type, source);
        }
    }
}
