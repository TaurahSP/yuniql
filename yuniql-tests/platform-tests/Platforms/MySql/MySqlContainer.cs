﻿using System;
using System.Collections.Generic;

namespace Yuniql.PlatformTests
{
    public class MySqlContainer : ContainerBase
    {
        public MySqlContainer()
        {
            Name = "mysql-test-infra";
            Image = "mysql";
            Tag = "latest";
            Env = new List<Tuple<string, string>> {
                    new Tuple<string, string>("MYSQL_ROOT_PASSWORD","P@ssw0rd!"),
                };
            Cmd = new List<string>() { "--default-authentication-plugin=mysql_native_password" };
            ExposedPorts = new List<string> { "3306" };
            MappedPorts = new List<Tuple<string, string>> {
                   new Tuple<string, string>("3306","3306")
                };
        }
    }
}
