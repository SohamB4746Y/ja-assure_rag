from typing import Optional, List

YES_NO_MAP = {
    "001": "Yes",
    "002": "No"
}

BUSINESS_TYPE_MAP = {
    "1": "Jewellery Retailer",
    "2": "Jewellery & Gold Manufacturer",
    "5": "Jewellery & Gold Bullion Distributor",
    "8": "Diamond Dealers",
    "10": "Money Changer",
    "11": "Remittance Services",
    "12": "Luxury Good Retailer",
    "13": "Luxury Watch Retailer",
    "34": "Pawnbrokers",
    "35": "Precious Stones Dealers"
}

INDUSTRY_MAP = {
    "1": "Jewellery & Gold",
    "2": "Diamond & Precious Stones",
    "6": "Money Services",
    "7": "Luxury Watches",
    "13": "Pawnbrokers"
}

CCTV_BACKUP_MAP = {
    "001": "Real-time backup (remote)",
    "002": "Real-time backup (on-site only)",
    "003": "Periodic backup (remote)",
    "004": "Periodic backup (on-site)",
    "005": "No backup",
    "006": "Others"
}

CCTV_RETENTION_MAP = {
    "001": "1 week",
    "002": "2 weeks",
    "003": "3 weeks",
    "004": "1 month",
    "005": "3 months",
    "006": "6 months",
    "007": "9 months",
    "008": "1 year",
    "009": "More than 1 year"
}

PREMISE_TYPE_MAP = {
    "001": "Office building",
    "002": "Shopping centre",
    "003": "Shop house",
    "004": "Others"
}

MATERIAL_MAP = {
    "001": "Concrete",
    "002": "Tiled",
    "003": "Metal",
    "004": "Wood"
}

DOOR_ACCESS_MAP = {
    "001": "Combinations",
    "002": "Fingerprint",
    "003": "Facial",
    "004": "Digital password",
    "005": "Key only",
    "006": "Others"
}

DOOR_MATERIAL_MAP = {
    "001": "Steel",
    "002": "Wooden",
    "003": "Glass",
    "004": "Others"
}

ROLLER_SHUTTER_MAP = {
    "001": "Roller shutter",
    "002": "Iron grill",
    "003": "Others"
}

ALARM_CONNECTION_MAP = {
    "001": "Security company",
    "002": "Landlord security",
    "003": "Police",
    "004": "Senior management"
}

ALARM_TYPE_MAP = {
    "001": "Door contacts",
    "002": "Roller shutter contacts",
    "003": "Infra-red beams",
    "004": "Ultrasonic detector",
    "005": "Motion detector",
    "006": "Seismic detector",
    "007": "Glass sensors",
    "008": "Portable panic button",
    "009": "Fixed type panic button",
    "010": "Others"
}

SAFE_GRADE_MAP = {
    "001": "Ungraded",
    "002": "Grade I",
    "003": "Grade II",
    "004": "Grade III",
    "005": "Grade IV",
    "006": "Grade V",
    "007": "Grade VI",
    "008": "Grade VII"
}

KEY_COMBINATION_MAP = {
    "001": "Key",
    "002": "Combination code",
    "003": "Both"
}

CLAIM_STATUS_MAP = {
    "001": "No claim within 3 years",
    "002": "Claims within the past 3 years"
}

POLICE_DISTANCE_MAP = {
    "001": "Less than 2 km",
    "002": "2-5 km",
    "003": "5-10 km",
    "004": "10-25 km",
    "005": "More than 25 km"
}

BACKGROUND_CHECK_MAP = {
    "001": "Contract in place + financial, criminal, social media checks once a year",
    "002": "Contract in place + criminal, social media checks once a year",
    "003": "Contract in place + Social media checks once a year",
    "004": "Contract in place"
}

STOCK_CHECK_MAP = {
    "001": "Daily",
    "002": "Weekly",
    "003": "Monthly",
    "004": "Less than 6 months",
    "005": "More than 6 months"
}

RECORDS_MAP = {
    "001": "Online",
    "002": "Offline"
}

CCTV_CAPABILITY_MAP = {
    "001": "Motion detection",
    "002": "Night vision",
    "003": "Others"
}

WALL_SHOWCASE_THICKNESS_MAP = {
    "001": "21 mm",
    "002": "17-19 mm",
    "003": "15 mm",
    "004": "11-13 mm",
    "005": "9-10 mm",
    "006": "Others"
}

SHOWCASE_PROTECTION_MAP = {
    "001": "Security glass",
    "002": "Laminated glass",
    "003": "Others"
}

COUNTER_THICKNESS_MAP = {
    "001": "19-21 mm",
    "002": "15-17 mm",
    "003": "12-14 mm",
    "004": "10-11 mm",
    "005": "6-9 mm",
    "006": "Others"
}

COUNTER_PROTECTION_MAP = {
    "001": "External vertical iron grilles and security glass",
    "002": "External vertical iron grilles and laminated glass",
    "003": "Internal lateral iron grilles and security glass",
    "004": "Internal lateral iron grilles and laminated",
    "005": "Security glass",
    "006": "Laminated glass",
    "007": "Others"
}

REAR_COUNTER_PROTECTION_MAP = {
    "001": "Iron grilles",
    "002": "Drawer with keylocks",
    "003": "Wooden flaps with keylocks",
    "004": "Wooden flaps with latch locks",
    "005": "Others"
}

DESTINATION_AIRPORT_MAP = {
    "001": "Bangkok airport",
    "002": "Hong Kong airport",
    "003": "Kuala Lumpur airport",
    "004": "Singapore airport",
    "005": "Tokyo airport",
    "006": "Sydney airport",
    "007": "Melbourne airport",
    "008": "Jakarta airport",
    "009": "All others"
}

EXHIBITION_INSURANCE_MAP = {
    "001": "Exhibition site risk only",
    "002": "Exhibition site risk including transit to/from by professional carrier"
}

REAR_DOOR_MAP = {
    "001": "Steel",
    "002": "Wooden",
    "003": "Others"
}

FIELD_MAPPINGS = {
    "business_profile": {
        "business_name_label": "Business Name",
        "mobile_number_label": "Mobile Number",
        "mailing_address_label": "Mailing Address",
        "office_telephone_label": "Office Telephone",
        "person_in_charge_label": "Person In Charge",
        "nature_of_business_label": "Nature of Business",
        "correspondence_email_label": "Correspondence Email",
        "business_registration_label": "Business Registration Number"
    },
    "cctv": {
        "recording_label": "CCTV Recording",
        "cctv_model_label": "CCTV Model",
        "cctv_brand_name_label": "CCTV Brand Name",
        "type_of_back_up_label": "Type of Backup",
        "type_of_backup_others_label": "Backup Type (Other)",
        "cctv_maintenance_contract_label": "CCTV Maintenance Contract",
        "additional_capability_label": "Additional Capability",
        "additional_capability_others_label": "Additional Capability (Other)",
        "retained_period_of_cctv_recording_label": "CCTV Recording Retention Period"
    },
    "transit_and_gaurds": {
        "usage_of_jaguar_transit_label": "Jaguar Transit Used",
        "do_you_use_armoured_vehicle_label": "Armoured Vehicle Used",
        "do_you_use_guards_at_premise_label": "Guards at Premise",
        "installed_gps_tracker_in_transit_bags_label": "GPS Tracker in Transit Bags",
        "do_you_use_armed_guards_during_transit_label": "Armed Guards During Transit",
        "installed_gps_tracker_in_transit_vehicles_label": "GPS Tracker in Transit Vehicles"
    },
    "claim_history": {
        "claim_history_label": "Claim History Status",
        "description_label": "Claim Description",
        "year_of_claim_label": "Year of Claim",
        "amount_of_claim_label": "Amount of Claim"
    },
    "physical_setup": {
        "premise_type_label": "Premise Type",
        "premise_type_others_label": "Premise Type (Other)",
        "roof_materials_label": "Roof Materials",
        "roof_materials_others_label": "Roof Materials (Other)",
        "wall_materials_label": "Wall Materials",
        "wall_materials_others_label": "Wall Materials (Other)",
        "floor_materials_label": "Floor Materials",
        "floor_materials_others_label": "Floor Materials (Other)"
    },
    "door_access": {
        "door_access_label": "Door Access Type",
        "door_access_others_label": "Door Access (Other)",
        "rear_door_label": "Rear Door Material",
        "rear_door_others_label": "Rear Door (Other)",
        "main_door_details_label": "Main Door Material",
        "main_door_others_label": "Main Door (Other)",
        "inner_door_details_label": "Inner Door Material",
        "inner_door_others_label": "Inner Door (Other)",
        "inner_door_iron_glass_label": "Inner Door Iron Glass",
        "inner_door_iron_wooden_label": "Inner Door Iron Wooden",
        "main_door_roll_and_iron_wood_label": "Main Door Roller/Iron Grill",
        "rear_door_roll_and_iron_wood_label": "Rear Door Roller/Iron Grill",
        "main_door_roll_and_iron_glass_label": "Main Door Roller/Iron Grill (Glass)"
    },
    "alarm": {
        "do_you_have_alarm_label": "Alarm Installed",
        "alarm_brand_name_label": "Alarm Brand Name",
        "alarm_model_label": "Alarm Model",
        "connection_type_label": "Alarm Connection Type",
        "type_of_alarm_system_label": "Type of Alarm System",
        "type_of_alarm_others_label": "Alarm Type (Other)",
        "under_maintenance_contract_label": "Under Maintenance Contract",
        "central_monitoring_stations_label": "Central Monitoring Station",
        "name_of_cms_company_label": "CMS Company Name"
    },
    "safe": {
        "safe_model_label": "Safe Model",
        "safe_weight_label": "Safe Weight",
        "safe_brand_name_label": "Safe Brand Name",
        "safe_time_locking_label": "Safe Time Locking",
        "grade_label": "Safe Grade",
        "certified_label": "Safe Certified",
        "key_combination_code_or_both_label": "Key/Combination/Both",
        "key_and_combination_code_held_by_separate_personnel_label": "Key and Code Held Separately"
    },
    "strong_room": {
        "do_you_have_a_strong_room_label": "Strong Room Available",
        "time_locking_label": "Time Locking",
        "time_locking_brand_label": "Time Locking Brand"
    },
    "display_showcases": {
        "wall_showcase_thickness_label": "Wall Showcase Thickness",
        "do_you_have_wall_showcase_label": "Wall Showcase Available",
        "wall_showcases_are_protected_by_label": "Wall Showcase Protection",
        "wall_showcases_are_protected_by_others_label": "Wall Showcase Protection (Other)"
    },
    "display_counters": {
        "counter_showcase_thickness_label": "Counter Showcase Thickness",
        "do_you_have_counter_showcase_label": "Counter Showcase Available",
        "counter_showcases_are_protected_by_label": "Counter Showcase Protection",
        "rear_counter_showcase_are_protected_by_label": "Rear Counter Protection"
    },
    "counter_show_case": {
        "thickness_of_counters_label": "Counter Thickness",
        "dw_counter_showcases_are_protected_by_label": "Display Window Counter Protection",
        "dw_counter_showcases_are_protected_by_others_label": "Display Window Counter Protection (Other)"
    },
    "records_keeping": {
        "records_maintained_in_label": "Records Maintained In",
        "do_you_keep_detailed_records_of_stock_movements_label": "Detailed Stock Records"
    },
    "additional_details": {
        "three_piece_rule_label": "Three Piece Rule",
        "the_nearest_police_station_label": "Nearest Police Station Distance",
        "standard_operating_procedure_label": "Standard Operating Procedure",
        "background_checks_for_all_employees_label": "Background Checks for Employees",
        "how_often_is_the_stock_check_carried_out_label": "Stock Check Frequency"
    },
    "display_window": {
        "display_window_form_title_label": "Display Window Form Title",
        "do_you_have_display_window_label": "Display Window Available",
        "display_window_protected_by_label": "Display Window Protection",
        "display_window_protected_by_others_label": "Display Window Protection (Other)",
        "display_window_thickness_label": "Display Window Thickness",
        "rear_display_window_protected_by_label": "Rear Display Window Protection",
        "rear_display_window_protected_by_others_label": "Rear Display Window Protection (Other)",
        "rear_display_window_thickness_label": "Rear Display Window Thickness"
    },
    "add_on_coverage": {
        "director_house_coverage_label": "Director House Coverage",
        "director_house_question_label": "Director House Question",
        "director_house_question_cctv_label": "Director House CCTV",
        "director_house_question_safe_label": "Director House Safe",
        "fidelity_guarantee_insurance_label": "Fidelity Guarantee Insurance",
        "fidelity_guarantee_total_staff_label": "Fidelity Guarantee Total Staff",
        "director_house_question_burglar_system_label": "Director House Burglar System",
        "fidelity_guarantee_insurance_add_coverage_label": "Fidelity Guarantee Add Coverage",
        "overseas_carrying_label": "Overseas Carrying",
        "sum_assured_limit_label": "Sum Assured Limit",
        "public_exhibitions_label": "Public Exhibitions",
        "destination_airport_label": "Destination Airport",
        "risk_location_address_label": "Risk Location Address",
        "authorized_company_name_label": "Authorized Company Name",
        "exhibtion_coverage_question_label": "Exhibition Coverage Question",
        "outward_entrustment_question_label": "Outward Entrustment Question",
        "exhibition_insurance_question_label": "Exhibition Insurance Question",
        "international_coverage_question_label": "International Coverage Question"
    },
    "shop_lifting": {
        "shop_lifting_label": "Shop Lifting Coverage"
    }
}


def decode_field(field_name: str, value: str) -> str:
    if value is None or value == "" or not isinstance(value, str):
        return value
    
    field_lower = field_name.lower()
    value_str = str(value).strip()
    
    if "nature_of_business" in field_lower or "businesstype" in field_lower:
        return BUSINESS_TYPE_MAP.get(value_str, value)
    
    if "industry" in field_lower:
        return INDUSTRY_MAP.get(value_str, value)
    
    if "type_of_back_up" in field_lower or "type_of_backup" in field_lower:
        return CCTV_BACKUP_MAP.get(value_str, value)
    
    if "retained_period" in field_lower or "cctv_retention" in field_lower:
        return CCTV_RETENTION_MAP.get(value_str, value)
    
    if "premise_type" in field_lower and "others" not in field_lower:
        return PREMISE_TYPE_MAP.get(value_str, value)
    
    if any(x in field_lower for x in ["roof_material", "wall_material", "floor_material"]) and "others" not in field_lower:
        return MATERIAL_MAP.get(value_str, value)
    
    if "door_access" in field_lower and "others" not in field_lower:
        return DOOR_ACCESS_MAP.get(value_str, value)
    
    if "main_door_details" in field_lower or "inner_door_details" in field_lower:
        return DOOR_MATERIAL_MAP.get(value_str, value)
    
    if "rear_door" in field_lower and "roll" not in field_lower and "others" not in field_lower:
        return REAR_DOOR_MAP.get(value_str, value)
    
    if "roll_and_iron" in field_lower:
        return ROLLER_SHUTTER_MAP.get(value_str, value)
    
    if "connection_type" in field_lower:
        return ALARM_CONNECTION_MAP.get(value_str, value)
    
    if "type_of_alarm_system" in field_lower:
        return ALARM_TYPE_MAP.get(value_str, value)
    
    if "grade" in field_lower and "safe" in field_lower or field_lower == "grade_label":
        return SAFE_GRADE_MAP.get(value_str, value)
    
    if "key_combination_code_or_both" in field_lower:
        return KEY_COMBINATION_MAP.get(value_str, value)
    
    if "claim_history_label" in field_lower:
        return CLAIM_STATUS_MAP.get(value_str, value)
    
    if "nearest_police" in field_lower:
        return POLICE_DISTANCE_MAP.get(value_str, value)
    
    if "background_check" in field_lower:
        return BACKGROUND_CHECK_MAP.get(value_str, value)
    
    if "stock_check" in field_lower or "how_often" in field_lower:
        return STOCK_CHECK_MAP.get(value_str, value)
    
    if "records_maintained" in field_lower:
        return RECORDS_MAP.get(value_str, value)
    
    if "additional_capability" in field_lower and "others" not in field_lower:
        return CCTV_CAPABILITY_MAP.get(value_str, value)
    
    if "wall_showcase_thickness" in field_lower or "display_window_thickness" in field_lower or "rear_display_window_thickness" in field_lower:
        return WALL_SHOWCASE_THICKNESS_MAP.get(value_str, value)
    
    if ("wall_showcases_are_protected" in field_lower or "display_window_protected" in field_lower or "rear_display_window_protected" in field_lower) and "others" not in field_lower:
        return SHOWCASE_PROTECTION_MAP.get(value_str, value)
    
    if "counter_showcase_thickness" in field_lower or "thickness_of_counters" in field_lower:
        return COUNTER_THICKNESS_MAP.get(value_str, value)
    
    if ("counter_showcases_are_protected" in field_lower or "dw_counter_showcases_are_protected" in field_lower) and "others" not in field_lower:
        return COUNTER_PROTECTION_MAP.get(value_str, value)
    
    if "rear_counter_showcase_are_protected" in field_lower:
        return REAR_COUNTER_PROTECTION_MAP.get(value_str, value)
    
    if "destination_airport" in field_lower:
        return DESTINATION_AIRPORT_MAP.get(value_str, value)
    
    if "exhibition_insurance_question" in field_lower:
        return EXHIBITION_INSURANCE_MAP.get(value_str, value)
    
    yes_no_fields = [
        "recording", "cctv_maintenance_contract", "do_you_have", "certified",
        "time_locking", "safe_time_locking", "key_and_combination_code_held",
        "three_piece_rule", "standard_operating_procedure", "director_house_question",
        "exhibtion_coverage", "outward_entrustment", "international_coverage",
        "fidelity_guarantee_insurance_add_coverage", "shop_lifting",
        "inner_door_iron", "do_you_use", "usage_of_jaguar", "installed_gps",
        "under_maintenance_contract", "central_monitoring", "do_you_keep"
    ]
    
    for pattern in yes_no_fields:
        if pattern in field_lower:
            return YES_NO_MAP.get(value_str, value)
    
    return value


def decode_record(data: dict, section: str) -> dict:
    if not isinstance(data, dict):
        return data
    
    decoded = {}
    for field_name, value in data.items():
        if isinstance(value, dict):
            decoded[field_name] = decode_record(value, section)
        elif isinstance(value, list):
            decoded[field_name] = [
                decode_record(item, section) if isinstance(item, dict) else decode_field(field_name, item)
                for item in value
            ]
        else:
            decoded[field_name] = decode_field(field_name, value)
    
    return decoded


def decode_dataframe(df, json_columns: Optional[List[str]] = None):
    """
    Decode all relevant columns in a DataFrame.
    
    For JSON columns, parses and decodes the nested structures.
    For regular columns, applies field-level decoding.
    
    Args:
        df: pandas DataFrame with raw coded values
        json_columns: List of column names containing JSON strings
        
    Returns:
        DataFrame with decoded human-readable values
    """
    import pandas as pd
    import json
    
    df = df.copy()
    
    # Default JSON columns based on common patterns
    if json_columns is None:
        json_columns = [
            col for col in df.columns
            if any(kw in col.lower() for kw in ["json", "_data", "_info", "_details"])
        ]
    
    # Decode JSON columns
    for col in json_columns:
        if col not in df.columns:
            continue
            
        def parse_and_decode(val):
            if pd.isna(val):
                return val
            try:
                parsed = json.loads(val) if isinstance(val, str) else val
                if isinstance(parsed, dict):
                    return decode_record(parsed)
                elif isinstance(parsed, list):
                    return [decode_record(item) if isinstance(item, dict) else item for item in parsed]
                return parsed
            except (json.JSONDecodeError, TypeError):
                return val
        
        df[col] = df[col].apply(parse_and_decode)
    
    # Decode regular columns
    for col in df.columns:
        if col in json_columns:
            continue
        
        # Apply field-level decoding
        df[col] = df[col].apply(lambda val: decode_field(col, val) if pd.notna(val) else val)
    
    return df
