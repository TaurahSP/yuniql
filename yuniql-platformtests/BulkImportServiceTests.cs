﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using Shouldly;
using System;
using Yuniql.Core;
using Yuniql.Extensibility;

namespace Yuniql.PlatformTests
{
    [TestClass]
    public class BulkImportServiceTests : TestBase
    {
        private string _targetPlatform;
        private ITestDataService _testDataService;

        private IMigrationServiceFactory _migrationServiceFactory;
        private ITraceService _traceService;
        private IEnvironmentService _environmentService;

        [TestInitialize]
        public void Setup()
        {
            //get target platform to tests from environment variable
            _targetPlatform = EnvironmentHelper.GetEnvironmentVariable("YUNIQL_TEST_TARGET_PLATFORM");
            if (string.IsNullOrEmpty(_targetPlatform))
            {
                _targetPlatform = "sqlserver";
            }

            //create test data service provider
            var testDataServiceFactory = new TestDataServiceFactory();
            _testDataService = testDataServiceFactory.Create(_targetPlatform);

            //create data service factory for migration proper
            _environmentService = new EnvironmentService();
            _traceService = new TraceService();
            _migrationServiceFactory = new MigrationServiceFactory(_environmentService, _traceService);
        }

        [TestMethod]
        public void Test_Bulk_Import()
        {
            //arrange
            var workingPath = GetOrCreateWorkingPath();
            var databaseName = new DirectoryInfo(workingPath).Name;
            var connectionString = _testDataService.GetConnectionString(databaseName);

            var localVersionService = new LocalVersionService(_traceService);
            localVersionService.Init(workingPath);

            localVersionService.IncrementMajorVersion(workingPath, null);
            string v100Directory = Path.Combine(workingPath, "v1.00");
            _testDataService.CreateScriptFile(Path.Combine(v100Directory, $"test_v1_00.sql"), _testDataService.CreateDbObjectScript($"test_v1_00"));

            localVersionService.IncrementMinorVersion(workingPath, null);
            string v101Directory = Path.Combine(workingPath, "v1.01");
            _testDataService.CreateScriptFile(Path.Combine(v101Directory, $"test_v1_01.sql"), _testDataService.CreateDbObjectScript($"test_v1_01"));
            _testDataService.CreateScriptFile(Path.Combine(v101Directory, $"test_v1_02_TestCsv.sql"), _testDataService.CreateBulkTableScript("TestCsv"));

            //act
            var migrationService = _migrationServiceFactory.Create(_targetPlatform);
            migrationService.Initialize(connectionString);
            migrationService.Run(workingPath, "v1.01", autoCreateDatabase: true);

            //assert
            _testDataService.CheckIfDbObjectExist(connectionString, "test_v1_00").ShouldBeTrue();
            _testDataService.CheckIfDbObjectExist(connectionString, "test_v1_01").ShouldBeTrue();
            _testDataService.CheckIfDbObjectExist(connectionString, "TestCsv").ShouldBeTrue();

            //arrange - add new version with csv files
            localVersionService.IncrementMinorVersion(workingPath, null);
            string v102Directory = Path.Combine(workingPath, "v1.02");
            _testDataService.CreateScriptFile(Path.Combine(v102Directory, $"test_v1_02.sql"), _testDataService.CreateDbObjectScript($"test_v1_02"));
            File.Copy(Path.Combine(Environment.CurrentDirectory, "TestCsv.csv"), Path.Combine(v102Directory, "TestCsv.csv"));

            //act - bulk load csv files
            migrationService.Run(workingPath, "v1.02", autoCreateDatabase: true);

            //assert
            _testDataService.CheckIfDbObjectExist(connectionString, "test_v1_02").ShouldBeTrue();
            _testDataService.CheckIfDbObjectExist(connectionString, "TestCsv").ShouldBeTrue();
        }

        [Ignore]
        [TestMethod]
        public void Test_Bulk_Import_Destination_Table_Does_Not_Exist_Throws_Error()
        {
            throw new NotImplementedException();
        }

        [Ignore]
        [TestMethod]
        public void Test_Bulk_Import_Mismatch_Columns_Throws_Error()
        {
            throw new NotImplementedException();
        }

    }
}
