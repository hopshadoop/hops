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

func parseConfigFiles() (map[string]string, error) {
	// HADOOP_HOME or HADOOP_CONF_DIR
	confDir := os.Getenv("HADOOP_CONF_DIR")
	if confDir == "" {
		confDir = os.Getenv("HADOOP_HOME")
		if confDir != "" {
			confDir = fmt.Sprintf("%s%s", confDir, "/etc/hadoop")
		}
	}

	if confDir == "" {
		return nil, fmt.Errorf("Failed to find HADOOP_CONF_DIR")
	}

	coreSite, coreErr := parseConfigFile(confDir, "core-site.xml")
	hdfsSite, hdfsErr := parseConfigFile(confDir, "hdfs-site.xml")

	if coreErr != nil && hdfsErr != nil {
		return nil, fmt.Errorf("failed to read core-site.xml and hdfs-site.xml. Errors: %v. %v", coreErr, hdfsErr)
	}

	if coreSite != nil && hdfsSite != nil {
		// combine and return
		for key, value := range hdfsSite {
			coreSite[key] = value
		}
		return coreSite, nil
	} else if coreSite != nil {
		return coreSite, nil
	} else if hdfsSite != nil {
		return hdfsSite, nil
	} else {
		return nil, fmt.Errorf("Programming error. Report bug")
	}
}

func parseConfigFile(confDir, file string) (map[string]string, error) {

	confFile := filepath.Join(confDir, file)

	xmlFile, err := os.Open(confFile)
	if err != nil {
		msg := fmt.Sprintf("Failed to read %s. Err: %v ", confFile, err)
		return nil, errors.New(msg)
	}
	defer xmlFile.Close()

	byteValue, _ := ioutil.ReadAll(xmlFile)
	var coreSiteProperties CoreSiteProperties
	err = xml.Unmarshal(byteValue, &coreSiteProperties)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to parse %s. Err: %v", confFile, err))
	}

	var properties = make(map[string]string)
	for _, prop := range coreSiteProperties.Properties {
		properties[prop.Name] = prop.Value
	}

	INFO("Using %s", confFile)

	return properties, nil
}

//------------------------------------------------------------------------------

func setTLSFromHadoopConfigFiles(userName string, opts *hdfs.ClientOptions) error {

	properties, err := parseConfigFiles()
	if err != nil {
		return err
	}

	ipcServerSslEnabledStr := properties["ipc.server.ssl.enabled"]
	hopsTlsSuperuserMaterialDirStr := properties["hops.tls.superuser-material-directory"]

	if ipcServerSslEnabledStr == "" {
		return fmt.Errorf("SSL params are not set in core-site.xml or hdfs-site.xml")
	}

	ipcServerSslEnabled, err := strconv.ParseBool(ipcServerSslEnabledStr)
	if err != nil {
		return fmt.Errorf("failed to parse ipc.server.ssl.enabled. Err: %v ", err)
	}

	if !ipcServerSslEnabled {
		return fmt.Errorf("SSL is not enabled in core-site.xml or hdfs-site.xml")
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
		msg := fmt.Sprintf("Failed to read TLS config from core-site.xml or hdfs-site.xml."+
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

func getNNURIFromConfigFiles() (string, error) {

	properties, err := parseConfigFiles()
	if err != nil {
		return "", err
	}

	fsDefaultFS := properties["fs.defaultFS"]
	if fsDefaultFS == "" {
		return "", fmt.Errorf("failed to read fs.defaultFS from core-site.xml or hdfs-site.xml")

	} else {
		INFO("fs.defaultFS from hadoop config files %s", fsDefaultFS)
		return fsDefaultFS, nil
	}
}

//------------------------------------------------------------------------------
