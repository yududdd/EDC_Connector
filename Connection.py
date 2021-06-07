__author__ = "Yu Du"
__Email__ = "yu.du@klserv.com"
__date__ = "May 4,2020"

##########################################################################################################
import codecs
from rwslib import RWSConnection
from rwslib.rws_requests import *
from rwslib.rws_requests.biostats_gateway import *
import xml.etree.ElementTree as ET


class APIConnector(object):
    def __init__(self, sub_domain, username, password):
        self.protocol = "https://"
        self.sub_domain = sub_domain
        self.username = username
        self.password = password
        self.main_domain = ".mdsol.com"

    def make_url(self):
        url = self.protocol + self.sub_domain + self.main_domain + "/RaveWebServices/"
        return url

    def authentication(self):
        """
        Set up the authentication process for connecting the Rave Web Services
        """
        rws = RWSConnection(self.protocol + self.sub_domain + self.main_domain, self.username, self.password)
        return rws

    def get_authStatus(self):
        client = self.authentication()
        client.send_request(VersionRequest())
        status = client.last_result.status_code
        return status

    def get_version(self):
        print("The current version is " + self.authentication().send_request(VersionRequest()))

    def get_studies(self):
        """
        Get a list of the Studies for the URL (this is predicated on the application being authorised in Rave)
        """
        client = self.authentication()
        studies = client.send_request(ClinicalStudiesRequest())
        return studies

    def get_subjects(self, study_oid):
        """
        Get a list of the subjects for a given study environment
        """
        client = self.authentication()
        # This is the same call as you would use with Basic Authentication
        project_name = study_oid.split("(")[0]
        project_environment = study_oid.split("(")[1].replace(")", "")
        subjects = client.send_request(
            StudySubjectsRequest(
                project_name=project_name,
                environment_name=project_environment,
                subject_key_type="SubjectUUID"
            )
        )
        return subjects

    def get_forms(self,study_oid):
        """
        make request to RWS for the metadata of clinical view columns for this study and environment
        :return: list of the forms
        """
        client = self.authentication()
        project_name = study_oid.split("(")[0]
        project_environment = study_oid.split("(")[1].replace(")", "")
        cv_metadata_odm = client.send_request(
            CVMetaDataRequest(project_name=project_name,
                              environment_name=project_environment,
                              rawsuffix='RAW')
        )
        # Parsing the xml to have the list of forms
        root = ET.fromstring(cv_metadata_odm)
        formls = list()
        # 3 layers of structure. Current solution is to use for loops iterate. Computation is cheap because n is small
        for child in root:
            for elements in child:
                if str(elements.tag) == "{http://www.cdisc.org/ns/odm/v1.3}MetaDataVersion":
                    for FormDef in elements:
                        formls.append(FormDef.get("OID"))

        return formls

    def get_studyDataset(self, study_oid):
        """
        Get clinical data set in ODM format for a given study environment
        """
        client = self.authentication()
        project_name = study_oid.split("(")[0]
        project_environment = study_oid.split("(")[1].replace(")", "")
        study_data = client.send_request(
            StudyDatasetRequest(
                project_name=project_name,
                environment_name=project_environment,
                dataset_type="regular"
            )
        )
        return study_data

    def get_form_data(self, study_oid, data_type, form_oid, data_format):
        """
         Retrieve data from Clinical Views for a single form. Data can be extracted from raw or regular views
         and can be formatted in XML or CSV.
         :param str study_oid: the study for particular environment, formatted as 'study(environment)'
         :param str form_oid: the particular form user want to retrieve data for
         :param str data_type: regular or raw
         :param str data_format: csv or xml
         """
        client = self.authentication()
        project_name = study_oid.split("(")[0]
        project_environment = study_oid.split("(")[1].replace(")", "")
        form_data = client.send_request(
            FormDataRequest(project_name, project_environment,
                            dataset_type=data_type, form_oid=form_oid, dataset_format=data_format)
        )
        return form_data

    def output_formdata(self, study_oid, data_type, form_oid):
        """
        Generated csv file version of particular form in the current directory
        :param form_oid: form which want to retrieve for
        :param dataset_type: raw data or regular
        :return: null
        """
        data = self.get_form_data(study_oid, data_type, form_oid, data_format='csv')
        filename = str(form_oid) + ".csv"
        with open(filename, 'w') as form_data:
            form_data.write(data)
        return data

    def output_odm_xml(self, study_oid, filename):
        """
        Generated xml formatted file for ODM data set with utf-8 encoding
        :return: null with generated xml file in the given directory
        """
        s = self.get_studyDataset(study_oid)
        unichr_captured = ""
        if not s.strip():
            return ""
        while s[0] != u"<":
            unichr_captured += s[0]
            s = s[1:]
        with codecs.open(filename, "w", encoding="utf-8") as f:
            f.write(s)
        return s


if __name__ == "__main__":
    # domain = input('Enter your domain: ')
    domain = ""
    username = ""
    password = ""
    conn = APIConnector(domain, username, password)
    # conn.output_odm_xml(, 'TETS.xml')
    conn.output_formdata('TEST', "raw", "AE")


