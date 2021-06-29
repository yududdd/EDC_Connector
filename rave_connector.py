__author__ = "Yu Du"
__Email__ = "yu.du@klserv.com"
__date__ = "May 4,2020"

##########################################################################################################
import codecs
import logging
import os

from rwslib import RWSConnection
from rwslib.rws_requests import ClinicalStudiesRequest
from rwslib.rws_requests import MetadataStudiesRequest
from rwslib.rws_requests import StudyDatasetRequest
from rwslib.rws_requests import StudyDraftsRequest
from rwslib.rws_requests import StudySubjectsRequest
from rwslib.rws_requests import StudyVersionRequest
from rwslib.rws_requests import StudyVersionsRequest
from rwslib.rws_requests import VersionRequest
from rwslib.rws_requests.biostats_gateway import CVMetaDataRequest
from rwslib.rws_requests.biostats_gateway import FormDataRequest
from rwslib.rws_requests.odm_adapter import AuditRecordsRequest

from clinchoice.common_connector import ConnectorBase
import xml.etree.ElementTree as ET


log = logging.getLogger(__name__)


class RaveAPIConnector(ConnectorBase):
    rws = None
     
    def __init__(self):
        self.protocol = "https://"
        self.main_domain = ".mdsol.com"

    def config(self, configuration):
        self.sub_domain = configuration["sub_domain"]
        self.username = configuration["username"]
        self.password = configuration["password"]

    def config0(self, sub_domain, username, password):
        self.sub_domain = sub_domain
        self.username = username
        self.password = password

    def make_url(self):
        url = self.protocol + self.sub_domain + self.main_domain + "/RaveWebServices/"
        return url

    def connect(self):
        """
        Set up the connect process for connecting the Rave Web Services
        """
        if (self.rws is None):
            self.rws = RWSConnection(self.protocol + self.sub_domain + self.main_domain, self.username, self.password)
        return self.rws

    def get_auth_status(self):
        client = self.connect()
        client.send_request(VersionRequest())
        status = client.last_result.status_code
        return status

    def write_to_xml(self, content , filename):
        s = content
        if not s.strip():
            return ""
        while s[0] != u"<":
            s = s[1:]
        with codecs.open(filename, "w", encoding="utf-8-sig") as f:
            f.write(s)

    def get_version(self):
        print("The current version is " + self.connect().send_request(VersionRequest()))

    def get_studies(self):
        """
        Get a list of the Studies for the URL (this is predicated on the application being authorised in Rave)
        """
        client = self.connect()
        studies = client.send_request(ClinicalStudiesRequest())
        return studies

    def get_subjects(self, study_oid):
        """
        Get a list of the subjects for a given study environment
        """
        client = self.connect()
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

    def get_forms(self, study_oid):
        """
        make request to RWS for the metadata of clinical view columns for this study and environment
        :return: list of the forms
        """
        client = self.connect()
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
                if elements.tag.endswith("}MetaDataVersion"):
                    for form_def in elements:
                        if form_def.tag.endswith("}FormDef"):
                            formls.append(form_def.get("OID"))

        return formls

    def get_study_dataset(self, study_oid):
        """
        Get clinical data set in ODM format for a given study environment
        """
        client = self.connect()
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

    def get_metadata_dataset(self):
        """
        Get metadata of studies
        """
        client = self.connect()
        return client.send_request(MetadataStudiesRequest())

    def get_audit_record(self, study_oid):
        """
        Get clinical data set in ODM format for a given study environment
        """
        client = self.connect()
        project_name = study_oid.split("(")[0]
        project_environment = study_oid.split("(")[1].replace(")", "")
        return client.send_request(
            AuditRecordsRequest(
                project_name=project_name,
                environment_name=project_environment
            )
        )

    def get_study_drafts(self, study_oid):
        client = self.connect()
        return client.send_request(
            StudyDraftsRequest(study_oid.split("(")[0])
        )

    def get_study_versions(self, study_oid):
        client = self.connect()
        return client.send_request(
            StudyVersionsRequest(study_oid.split("(")[0])
        )

    def get_study_version(self, study_oid, version_oid):
        client = self.connect()
        return client.send_request(
            StudyVersionRequest(study_oid.split("(")[0], version_oid)
        )

    def get_form_data(self, study_oid, data_type, form_oid, data_format):
        """
         Retrieve data from Clinical Views for a single form. Data can be extracted from raw or regular views
         and can be formatted in XML or CSV.
         :param str study_oid: the study for particular environment, formatted as 'study(environment)'
         :param str form_oid: the particular form user want to retrieve data for
         :param str data_type: regular or raw
         :param str data_format: csv or xml
         """
        client = self.connect()
        project_name = study_oid.split("(")[0]
        project_environment = study_oid.split("(")[1].replace(")", "")
        form_data = client.send_request(
            FormDataRequest(project_name, project_environment,
                            dataset_type=data_type, form_oid=form_oid, dataset_format=data_format)
        )
        return form_data.rstrip('EOF').rstrip()

    def output_form_data(self, study_oid, data_type, form_oid, target_folder="."):
        """
        Generated csv file version of particular form in the current directory
        :param form_oid: form which want to retrieve for
        :param dataset_type: raw data or regular
        :return: null
        """
        log.info(f"Reading form [{form_oid}] from study [{study_oid}]")
        try:
            data = self.get_form_data(study_oid, data_type, form_oid, data_format='csv')
            filename = os.path.join(target_folder, str(form_oid) + ".csv") 
            with open(filename, 'w', encoding="utf-8", newline='') as form_data:
                form_data.write(data)
            return data
        except:
            log.error(f"Failed to read form [{form_oid}] from study [{study_oid}]")
            return None
        

    def output_all_forms(self, study_oid, target_folder):
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        all_forms = self.get_forms(study_oid)
        for form_oid in all_forms:
            self.output_form_data(study_oid,"regular",form_oid, target_folder)
        
    def output_odm_xml(self, study_oid, filename):
        """
        Generated xml formatted file for ODM data set with utf-8 encoding 
        :return: null with generated xml file in the given directory
        """
        s = self.get_study_dataset(study_oid)
        self.write_to_xml(s, filename)
        return s
    
    def output_audit_xml(self, study_oid, filename):
        """
        Generated xml formatted file for ODM data set with utf-8 encoding 
        :return: null with generated xml file in the given directory
        """
        s = self.get_audit_record(study_oid)
        self.write_to_xml(s, filename)
        return s

    def output_study_crf_drafts(self, study_oid, filename):
        s = self.get_study_drafts(study_oid)
        self.write_to_xml(str(s), filename)
        return s
        
    def output_study_version(self, study_oid, version_oid, filename):
        s = self.get_study_version(study_oid, version_oid);
        self.write_to_xml(s, filename)
        return s
