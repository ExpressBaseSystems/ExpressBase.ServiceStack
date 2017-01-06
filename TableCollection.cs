using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class TableCollection: Dictionary<int, string>
    {
        private int _id;
        private string _tablename;

        public int Id
        {
            get{ return _id;}
            set{ _id = value;}
        }

        public string TableName
        {
            get{ return _tablename;}
            set{ _tablename = value;}
        }

        new public void Add(int id,string tname)                                      //to add  new permissions into permission collection
        {
            if (!(this.ContainsKey(id) && this.ContainsValue(tname)))
                base.Add(id,tname);
            else
                throw new TableAlreadyFoundInCollectionException();
        }

        public void Remove(int id, string tname)                                   //to delete  a permission from permission collection
        {
            if (!(this.ContainsKey(id) && this.ContainsValue(tname)))
                base.Remove(id);
            else
                throw new TableNotFoundInCollectionException();
        }
    }

    internal class TableNotFoundInCollectionException : Exception
    {
        public TableNotFoundInCollectionException()
        {
        }

        public TableNotFoundInCollectionException(string message) : base(message)
        {
        }

        public TableNotFoundInCollectionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    internal class TableAlreadyFoundInCollectionException : Exception
    {
        public TableAlreadyFoundInCollectionException()
        {
        }

        public TableAlreadyFoundInCollectionException(string message) : base(message)
        {
        }

        public TableAlreadyFoundInCollectionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
