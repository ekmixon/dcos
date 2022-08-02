class FetchError(Exception):

    def __init__(self, url, out_filename, base_exception, rm_failed):
        self.url = url
        self.out_filename = out_filename
        self.base_exception = base_exception
        self.rm_failed = rm_failed

    def __str__(self):
        msg = f"Problem fetching {self.url} to {self.out_filename} because of {self.base_exception}."


        if self.rm_failed:
            msg += " Unable to remove partial download. Future builds may have problems because of it.".format(
                self.rm_failed)

        return msg


class IncompleteDownloadError(Exception):

    def __init__(self, url, total_bytes_read, content_length):
        self.url = url
        self.total_bytes_read = total_bytes_read
        self.content_length = content_length

    def __str__(self):
        return f"Problem fetching {self.url} - bytes read {self.total_bytes_read} does not match content-length {self.content_length}"


class InstallError(Exception):
    pass


class PackageError(Exception):
    pass


class PackageNotFound(PackageError):
    pass


class ValidationError(Exception):
    pass


class PackageConflict(ValidationError):
    pass
