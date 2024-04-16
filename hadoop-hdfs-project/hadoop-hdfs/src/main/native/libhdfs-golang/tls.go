package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/colinmarc/hdfs/v2"
)

//------------------------------------------------------------------------------

type CoreSiteProperties struct {
	XMLName    xml.Name           `xml:"configuration"`
	Properties []CoreSiteProperty `xml:"property"`
}

//------------------------------------------------------------------------------

type CoreSiteProperty struct {
	XMLName     xml.Name `xml:"property"`
	Name        string   `xml:"name"`
	Value       string   `xml:"value"`
	Description string   `xml:"Description"`
}

//------------------------------------------------------------------------------

func setTLSFromEnvVariables(opts *hdfs.ClientOptions) error {

	libhdfsRootCABundle := os.Getenv(LIBHDFS_ROOT_CA_BUNDLE)

	libhdfsClientCertificate := os.Getenv(LIBHDFS_CLIENT_CERTIFICATE)

	libhdfsClientKey := os.Getenv(LIBHDFS_CLIENT_KEY)

	if libhdfsRootCABundle == "" ||
		libhdfsClientCertificate == "" ||
		libhdfsClientKey == "" {
		return errors.New("Failed to read TLS config from environment variables")
	}

	_, err1 := os.Stat(libhdfsRootCABundle)
	_, err2 := os.Stat(libhdfsClientCertificate)
	_, err3 := os.Stat(libhdfsClientKey)

	if err1 != nil || err2 != nil || err3 != nil {
		msg := fmt.Sprintf("Failed to read TLS config from environment variables."+
			" Environment variables not set correctly."+
			" RootCABundle: %s, ClientCertificate: %s, ClientKey: %s",
			libhdfsRootCABundle, libhdfsClientCertificate, libhdfsClientCertificate)
		return errors.New(msg)
	} else {
		opts.TLS = true
		opts.RootCABundle = libhdfsRootCABundle
		opts.ClientCertificate = libhdfsClientCertificate
		opts.ClientKey = libhdfsClientKey
		INFO("TLS Enabled. RootCABundle: %s, ClientCertificate: %s, ClientKey: %s   ",
			opts.RootCABundle, opts.ClientCertificate, opts.ClientKey)
	}

	return nil
}

//------------------------------------------------------------------------------

func setTLSFromHadoopConfigFiles(userName string, opts *hdfs.ClientOptions) error {

	// HADOOP_HOME or HADOOP_CONF_DIR
	confDir := os.Getenv("HADOOP_CONF_DIR")
	if confDir == "" {
		confDir = os.Getenv("HADOOP_HOME")
		if confDir != "" {
			confDir = fmt.Sprintf("%s%s", confDir, "/etc/hadoop")
		}
	}

	if confDir == "" {
		return fmt.Errorf("Failed to find core-site.xml")
	}

	confFile := filepath.Join(confDir, "core-site.xml")

	return parseCoreSite(confFile, userName, opts)
}

func parseCoreSite(path string, userName string, opts *hdfs.ClientOptions) error {

	xmlFile, err := os.Open(path)
	if err != nil {
		msg := fmt.Sprintf("Failed to read TLS config from core-site.xml. Err: %v ", err)
		return errors.New(msg)
	}
	defer xmlFile.Close()

	byteValue, _ := ioutil.ReadAll(xmlFile)
	var coreSiteProperties CoreSiteProperties
	err = xml.Unmarshal(byteValue, &coreSiteProperties)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to parse core-site.xml. Err: %v", err))
	}

	var properties = make(map[string]string)
	for _, prop := range coreSiteProperties.Properties {
		properties[prop.Name] = prop.Value
	}

	ipcServerSslEnabledStr := properties["ipc.server.ssl.enabled"]
	hopsTlsSuperuserMaterialDirStr := properties["hops.tls.superuser-material-directory"]

	if ipcServerSslEnabledStr == "" {
		return fmt.Errorf("SSL params are not set in %s ", path)
	}

	ipcServerSslEnabled, err := strconv.ParseBool(ipcServerSslEnabledStr)
	if err != nil {
		return fmt.Errorf("failed to parse ipc.server.ssl.enabled in %s. Err: %v ", path, err)
	}

	if !ipcServerSslEnabled {
		return fmt.Errorf("SSL is not enabled in %s ", path)
	}

	// replace ${USER} if preset
	if strings.Contains(hopsTlsSuperuserMaterialDirStr, "${USER}") {
		hopsTlsSuperuserMaterialDirStr = strings.Replace(hopsTlsSuperuserMaterialDirStr, "${USER}", userName, -1)
	}

	if _, err := os.Stat(hopsTlsSuperuserMaterialDirStr); err != nil {
		return fmt.Errorf("failed to stat hops.tls.superuser-material-directory: %s. Err: %v ", hopsTlsSuperuserMaterialDirStr, err)
	}

	// check for files
	libhdfsRootCABundle := fmt.Sprintf("%s/%s", hopsTlsSuperuserMaterialDirStr, "hops_root_ca.pem")
	libhdfsClientCertificate := fmt.Sprintf("%s/%s%s", hopsTlsSuperuserMaterialDirStr, userName, "_certificate_bundle.pem")
	libhdfsClientKey := fmt.Sprintf("%s/%s%s", hopsTlsSuperuserMaterialDirStr, userName, "_priv.pem")

	_, err1 := os.Stat(libhdfsRootCABundle)
	_, err2 := os.Stat(libhdfsClientCertificate)
	_, err3 := os.Stat(libhdfsClientKey)

	if err1 != nil || err2 != nil || err3 != nil {
		msg := fmt.Sprintf("Failed to read TLS config from core-site.xml."+
			" RootCABundle: %s, ClientCertificate: %s, ClientKey: %s",
			libhdfsRootCABundle, libhdfsClientCertificate, libhdfsClientCertificate)
		return errors.New(msg)
	} else {
		opts.TLS = true
		opts.RootCABundle = libhdfsRootCABundle
		opts.ClientCertificate = libhdfsClientCertificate
		opts.ClientKey = libhdfsClientKey
		INFO("TLS Enabled. RootCABundle: %s, ClientCertificate: %s, ClientKey: %s",
			opts.RootCABundle, opts.ClientCertificate, opts.ClientKey)
	}

	return nil
}

//------------------------------------------------------------------------------
