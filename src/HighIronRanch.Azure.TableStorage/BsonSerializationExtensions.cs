﻿using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace HighIronRanch.Azure.TableStorage
{
    public static class BsonSerializationExtensions
    {
        public static byte[] ToBson(this object entity)
        {
            using (var ms = new MemoryStream())
            { 
                using (var writer = new BsonDataWriter(ms))
                {
                    var serializer = new JsonSerializer {DateTimeZoneHandling = DateTimeZoneHandling.Utc};
                    serializer.Serialize(writer, entity);
                }

                return ms.ToArray();
            }
        }

        public static object FromBson(this byte[] bson, Type type)
        {
            using (var ms = new MemoryStream(bson))
            {
                using (var reader = new BsonDataReader(ms))
                {
                    var serializer = new JsonSerializer { DateTimeZoneHandling = DateTimeZoneHandling.Utc };
                    return serializer.Deserialize(reader, type);
                }
            }
        }

        public static T FromBson<T>(this byte[] bson)
        {
            return (T)FromBson(bson, typeof (T));
        }
    }
}