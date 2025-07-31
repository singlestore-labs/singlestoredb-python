"""Private pydoctor customization code in order to exclude the package
singlestoredb.docstring.tests from the API documentation. Based on Twisted code.
"""
# pylint: disable=invalid-name

try:
    from pydoctor.model import Documentable, PrivacyClass, System
except ImportError:
    pass
else:

    class HidesTestsPydoctorSystem(System):
        """A PyDoctor "system" used to generate the docs."""

        def privacyClass(self, documentable: Documentable) -> PrivacyClass:
            """Report the privacy level for an object. Hide the module
            'singlestoredb.docstring.tests'.
            """
            if documentable.fullName().startswith('singlestoredb.docstring.tests'):
                return PrivacyClass.HIDDEN
            return super().privacyClass(documentable)
