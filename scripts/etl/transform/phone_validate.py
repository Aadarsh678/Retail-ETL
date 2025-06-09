import phonenumbers
from phonenumbers import NumberParseException
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def validate_any_phone(phone_str):
    if phone_str is None:
        return None
    phone_str = phone_str.strip()
    try:
        # First: try parsing as international format (expects leading +)
        parsed = phonenumbers.parse(phone_str, None)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        else:
            return None
    except NumberParseException:
        # Fallback: try parsing with US as default (or pick any default)
        try:
            parsed = phonenumbers.parse(phone_str, "US")
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            else:
                return None
        except NumberParseException:
            return None

validate_any_phone_udf = udf(validate_any_phone, StringType())
