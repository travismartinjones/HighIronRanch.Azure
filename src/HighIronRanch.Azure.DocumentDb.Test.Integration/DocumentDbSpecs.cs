using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using developwithpassion.specifications.rhinomocks;
using HighIronRanch.Core.Repositories;
using HighIronRanch.Core.Services;
using Machine.Specifications;

namespace HighIronRanch.Azure.DocumentDb.Test.Integration
{
    public class DocumentDbSpecs
    {
        [Subject(typeof(DocumentDbWritableReadModelRepository))]
        public class Concern : Observes<DocumentDbWritableReadModelRepository>
        {
            public static readonly string TestQueueName = "TestQueueName";

            private static DocumentDbSettings _settings;

            private Establish ee = () =>
            {
                _settings = DocumentDbSettings.Create();

                depends.on<IDocumentDbSettings>(_settings);

                var logger = new TraceLogger();
                depends.on<ILogger>(logger);

                var factory = new DocumentDbClientFactory(logger);
                depends.on<IDocumentDbClientFactory>(factory);
            };
        }

        public class CleaningConcern : DocumentDbSpecs.Concern
        {
            private Cleanup cc = () =>
            {
                sut.DeleteDatabaseAsync().Wait();
            };
        }

        public class OneDocumentConcern : CleaningConcern
        {
            protected static TestDocument _testDocument;

            private Establish eee = () =>
            {
                _testDocument = new TestDocument()
                {
                    Id = Guid.NewGuid(),
                    Payload = 13
                };
            };

        }

        public class When_inserting_a_document_to_a_collection : OneDocumentConcern
        {
            private static IList<TestDocument> _results;

            private Because of = () =>
            {
                var list = new List<TestDocument> {_testDocument};
                sut.Insert<TestDocument>(list);
                _results = sut.Get<TestDocument>().ToList();
            };

            private It should_have_one_document = () => _results.Count.ShouldEqual(1);
            private It should_contain_the_document = () => _results[0].Id.ShouldEqual(_testDocument.Id);
        }

        public class When_saving_a_document_to_a_collection : OneDocumentConcern
        {
            private static IList<TestDocument> _results;

            private Because of = () =>
            {
                var list = new List<TestDocument> { _testDocument };
                sut.Insert<TestDocument>(list);
                _testDocument.Payload++;
                sut.Save(_testDocument);
                _results = sut.Get<TestDocument>().ToList();
            };

            private It should_have_one_document = () => _results.Count.ShouldEqual(1);
            private It should_contain_the_document = () => _results[0].Id.ShouldEqual(_testDocument.Id);
            private It should_contain_the_updated_document = () => _results[0].Payload.ShouldEqual(_testDocument.Payload);
        }

        public class When_deleting_a_document_from_a_collection : OneDocumentConcern
        {
            private static IList<TestDocument> _results;

            private Because of = () =>
            {
                var list = new List<TestDocument> { _testDocument };
                sut.Insert<TestDocument>(list);
                sut.Delete(_testDocument);
                _results = sut.Get<TestDocument>().ToList();
            };

            private It should_have_no_documents = () => _results.Count.ShouldEqual(0);
        }

        public class When_getting_a_document_from_a_collection_by_id : OneDocumentConcern
        {
            private static TestDocument _testDocument2;

            private static TestDocument _result;

            private Establish that = () =>
            {
                _testDocument2 = new TestDocument()
                {
                    Id = Guid.NewGuid()
                };
            };

            private Because of = () =>
            {
                var list = new List<TestDocument> { _testDocument, _testDocument2 };
                sut.Insert<TestDocument>(list);
                _result = sut.Get<TestDocument>(_testDocument.Id);
            };

            private It should_get_the_document = () => _result.Id.ShouldEqual(_testDocument.Id);
        }

        public class TestDocument : IViewModel
        {
            public Guid Id { get; set; }
            public int Payload { get; set; }
        }
    }
}