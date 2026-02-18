"""
Complete Decoder Mappings for JA Assure RAG System

Every coded field in the database is mapped here to its human-readable value.
Organized by section, matching the exact specifications provided.

RULES:
- Fields with code values (001, 002, etc.) -> decoded via maps
- Fields marked "Add the Value directly" -> returned as-is (no map needed)
- Yes/No fields -> YES_NO_MAP
- Numeric fields (weight, staff count, currency) -> returned as-is
"""
from __future__ import annotations
from typing import Optional, List


# ===========================================================================
# UNIVERSAL MAPS (used across multiple sections)
# ===========================================================================

# Yes/No map - handles "001"/"002", "1"/"2", and boolean formats
YES_NO_MAP = {
    "001": "Yes",
    "002": "No",
    "1": "Yes",
    "2": "No",
    "true": "Yes",
    "false": "No"
}


# ===========================================================================
# BUSINESS IDENTITY MAPS
# ===========================================================================

# industry_id -> Static values from folder structure
INDUSTRY_MAP = {
    "1": "Jewellery & Gold",
    "2": "Diamond & Precious Stones",
    "6": "Money Services",
    "7": "Luxury Watches",
    "13": "Pawnbrokers"
}

# businesstype_id -> Malaysia specific
BUSINESS_TYPE_MAP = {
    "1": "Jewellery Retailer",
    "2": "Jewellery & Gold Manufacturer",
    "3": "Jewellery & Gold Wholesaler",
    "5": "Jewellery & Gold Bullion Distributor",
    "8": "Diamond Dealers",
    "10": "Money Changer",
    "11": "Remittance Services",
    "12": "Luxury Good Retailer",
    "13": "Luxury Watch Retailer",
    "34": "Pawnbrokers",
    "35": "Precious Stones Dealers"
}


# ===========================================================================
# PHYSICAL SETUP MAPS
# ===========================================================================

PREMISE_TYPE_MAP = {
    "001": "Office building",
    "002": "Shopping centre",
    "003": "Shop house",
    "004": "Others"
}

# Used for roof_materials, wall_materials, floor_materials
MATERIAL_MAP = {
    "001": "Concrete",
    "002": "Tiled",
    "003": "Metal",
    "004": "Wood"
}


# ===========================================================================
# CCTV MAPS
# ===========================================================================

CCTV_BACKUP_MAP = {
    "001": "Real-time backup - remote",
    "002": "Real-time backup - on site only",
    "003": "Periodic backup - remote",
    "004": "Periodic backup - onsite",
    "005": "No backup",
    "006": "Others"
}

CCTV_CAPABILITY_MAP = {
    "001": "Motion detection",
    "002": "Night vision",
    "003": "Others"
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


# ===========================================================================
# DOOR ACCESS MAPS
# ===========================================================================

DOOR_ACCESS_MAP = {
    "001": "Combinations",
    "002": "Finger print",
    "003": "Facial",
    "004": "Digital password",
    "005": "Key only",
    "006": "Others"
}

# main_door_details_label, inner_door_details_label
DOOR_MATERIAL_MAP = {
    "001": "Steel",
    "002": "Wooden",
    "003": "Glass",
    "004": "Others"
}

# rear_door_label
REAR_DOOR_MAP = {
    "001": "Steel",
    "002": "Wooden",
    "003": "Others"
}

# main_door_roll_and_iron_wood_label, rear_door_roll_and_iron_wood_label,
# main_door_roll_and_iron_glass_label
ROLLER_SHUTTER_MAP = {
    "001": "Roller shutter",
    "002": "Iron grill",
    "003": "Others"
}


# ===========================================================================
# ALARM MAPS
# ===========================================================================

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


# ===========================================================================
# SAFE MAPS
# ===========================================================================

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


# ===========================================================================
# DISPLAY SHOWCASES / COUNTERS / WINDOWS MAPS
# ===========================================================================

# wall_showcase_thickness_label, display_window_thickness_label,
# rear_display_window_thickness_label
SHOWCASE_THICKNESS_MAP = {
    "001": "21 mm",
    "002": "17-19 mm",
    "003": "15 mm",
    "004": "11-13 mm",
    "005": "9-10 mm",
    "006": "Others"
}

# wall_showcases_are_protected_by_label, display_window_protected_by_label,
# rear_display_window_protected_by_label
SHOWCASE_PROTECTION_MAP = {
    "001": "Security glass",
    "002": "Laminated glass",
    "003": "Others"
}

# counter_showcase_thickness_label, thickness_of_counters_label
COUNTER_THICKNESS_MAP = {
    "001": "19-21 mm",
    "002": "15-17 mm",
    "003": "12-14 mm",
    "004": "10-11 mm",
    "005": "6-9 mm",
    "006": "Others"
}

# counter_showcases_are_protected_by_label
COUNTER_PROTECTION_MAP = {
    "001": "External vertical iron grilles and security glass",
    "002": "External vertical iron grilles and laminated glass",
    "003": "Internal lateral iron grilles and security glass",
    "004": "Internal lateral iron grilles and laminated",
    "005": "Security glass",
    "006": "Laminated glass"
}

# dw_counter_showcases_are_protected_by_label
DW_COUNTER_PROTECTION_MAP = {
    "001": "External vertical iron grilles and security glass",
    "002": "External vertical iron grilles and laminated glass",
    "003": "Internal lateral iron grilles and security glass",
    "004": "Internal lateral iron grilles and laminated",
    "005": "Security glass",
    "006": "Laminated glass",
    "007": "Others"
}

# rear_counter_showcase_are_protected_by_label
REAR_COUNTER_PROTECTION_MAP = {
    "001": "Iron grilles",
    "002": "Drawer with keylocks",
    "003": "Wooden flaps with keylocks",
    "004": "Wooden flaps with latch locks",
    "005": "Others"
}


# ===========================================================================
# ADDITIONAL DETAILS MAPS
# ===========================================================================

POLICE_DISTANCE_MAP = {
    "001": "Less than 2 Km",
    "002": "Within 2-5 Kms",
    "003": "5-10 Kms",
    "004": "Within 10-25 Kms",
    "005": "More than 25 Kms"
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


# ===========================================================================
# CLAIMS MAPS
# ===========================================================================

CLAIM_STATUS_MAP = {
    "001": "No claim within 3 years",
    "002": "Claims within the past 3 years"
}


# ===========================================================================
# ADD-ON COVERAGE MAPS
# ===========================================================================

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


# ===========================================================================
# FIELD -> MAP ROUTING TABLE
# Maps every coded field name to its exact decoder map.
# ===========================================================================

# Fields that are "Add the Value directly" - never decode these
PASSTHROUGH_FIELDS = {
    "premise_type_others_label",
    "roof_materials_others_label",
    "wall_materials_others_label",
    "floor_materials_others_label",
    "cctv_model_label",
    "cctv_brand_name_label",
    "type_of_backup_others_label",
    "additional_capability_others_label",
    "door_access_others_label",
    "others_label",
    "rear_door_others_label",
    "main_door_others_label",
    "inner_door_others_label",
    "alarm_brand_name_label",
    "alarm_model_label",
    "type_of_alarm_others_label",
    "name_of_cms_company_label",
    "safe_model_label",
    "safe_weight_label",
    "safe_brand_name_label",
    "time_locking_brand_label",
    "wall_showcases_are_protected_by_others_label",
    "dw_counter_showcases_are_protected_by_others_label",
    "display_window_protected_by_others_label",
    "rear_display_window_protected_by_others_label",
    "display_window_form_title_label",
    "director_house_coverage_label",
    "fidelity_guarantee_insurance_label",
    "fidelity_guarantee_total_staff_label",
    "overseas_carrying_label",
    "sum_assured_limit_label",
    "public_exhibitions_label",
    "risk_location_address_label",
    "authorized_company_name_label",
    "description_label",
    "year_of_claim_label",
    "amount_of_claim_label",
    "business_name_label",
    "mobile_number_label",
    "mailing_address_label",
    "office_telephone_label",
    "person_in_charge_label",
    "correspondence_email_label",
    "business_registration_label",
    "property_label",
    "risk_address_label",
    # Premise sub-limit fields - all "Add the Value"
    "maximum_value_kept_as_display_at_during_business_hours_aw_label",
    "maximum_value_kept_as_display_at_during_business_hours_1ar_label",
    "maximum_value_kept_as_display_at_during_business_hours_1pd_label",
    "maximum_value_kept_as_display_at_during_business_hours_aws_label",
    "maximum_value_kept_as_display_at_during_after_business_hours_aw_label",
    "maximum_value_kept_as_display_at_during_after_business_hours_1ar_label",
    "maximum_value_kept_as_display_at_during_after_business_hours_1pd_label",
    "maximum_value_kept_as_display_at_during_after_business_hours_aws_label",
    # Sum assured value fields
    "maximum_stock_in_premises_label",
    "value_of_stock_out_of_safe_label",
    "maximum_stock_during_transit_label",
    "maximum_cash_in_premises_label",
    "maximum_foreign_currency_label",
    "value_of_cash_in_premise_label",
    "value_of_pledged_stock_in_premise_label",
    "value_of_non_pledged_stock_in_premise_label",
    "maximum_stock_foreign_currency_in_premise_label",
    "maximum_stock_foreign_currency_in_transit_label",
    "value_of_stock_in_transit_label",
}

# Explicit field name -> map routing (exact match on field name)
FIELD_DECODE_TABLE: dict[str, dict] = {
    # --- Business identity ---
    "nature_of_business_label": BUSINESS_TYPE_MAP,
    "businesstype_id_label": BUSINESS_TYPE_MAP,
    "industry_id_label": INDUSTRY_MAP,

    # --- Physical setup ---
    "premise_type_label": PREMISE_TYPE_MAP,
    "roof_materials_label": MATERIAL_MAP,
    "wall_materials_label": MATERIAL_MAP,
    "floor_materials_label": MATERIAL_MAP,

    # --- CCTV ---
    "recording_label": YES_NO_MAP,
    "type_of_back_up_label": CCTV_BACKUP_MAP,
    "cctv_maintenance_contract_label": YES_NO_MAP,
    "additional_capability_label": CCTV_CAPABILITY_MAP,
    "retained_period_of_cctv_recording_label": CCTV_RETENTION_MAP,

    # --- Door access ---
    "door_access_label": DOOR_ACCESS_MAP,
    "rear_door_label": REAR_DOOR_MAP,
    "main_door_details_label": DOOR_MATERIAL_MAP,
    "inner_door_details_label": DOOR_MATERIAL_MAP,
    "inner_door_iron_glass_label": YES_NO_MAP,
    "inner_door_iron_wooden_label": YES_NO_MAP,
    "main_door_roll_and_iron_wood_label": ROLLER_SHUTTER_MAP,
    "rear_door_roll_and_iron_wood_label": ROLLER_SHUTTER_MAP,
    "main_door_roll_and_iron_glass_label": ROLLER_SHUTTER_MAP,

    # --- Alarm ---
    "do_you_have_alarm_label": YES_NO_MAP,
    "connection_type_label": ALARM_CONNECTION_MAP,
    "type_of_alarm_system_label": ALARM_TYPE_MAP,
    "under_maintenance_contract_label": YES_NO_MAP,
    "central_monitoring_stations_label": YES_NO_MAP,

    # --- Safe ---
    "safe_time_locking_label": YES_NO_MAP,
    "grade_label": SAFE_GRADE_MAP,
    "certified_label": YES_NO_MAP,
    "key_combination_code_or_both_label": KEY_COMBINATION_MAP,
    "key_and_combination_code_held_by_separate_personnel_label": YES_NO_MAP,

    # --- Strong room ---
    "do_you_have_a_strong_room_label": YES_NO_MAP,
    "time_locking_label": YES_NO_MAP,

    # --- Display showcases ---
    "wall_showcase_thickness_label": SHOWCASE_THICKNESS_MAP,
    "do_you_have_wall_showcase_label": YES_NO_MAP,
    "wall_showcases_are_protected_by_label": SHOWCASE_PROTECTION_MAP,

    # --- Display counters ---
    "counter_showcase_thickness_label": COUNTER_THICKNESS_MAP,
    "do_you_have_counter_showcase_label": YES_NO_MAP,
    "counter_showcases_are_protected_by_label": COUNTER_PROTECTION_MAP,
    "rear_counter_showcase_are_protected_by_label": REAR_COUNTER_PROTECTION_MAP,

    # --- Counter show case ---
    "thickness_of_counters_label": COUNTER_THICKNESS_MAP,
    "dw_counter_showcases_are_protected_by_label": DW_COUNTER_PROTECTION_MAP,

    # --- Display window ---
    "do_you_have_display_window_label": YES_NO_MAP,
    "display_window_protected_by_label": SHOWCASE_PROTECTION_MAP,
    "display_window_thickness_label": SHOWCASE_THICKNESS_MAP,
    "rear_display_window_protected_by_label": SHOWCASE_PROTECTION_MAP,
    "rear_display_window_thickness_label": SHOWCASE_THICKNESS_MAP,

    # --- Transit and guards ---
    "usage_of_jaguar_transit_label": YES_NO_MAP,
    "do_you_use_armoured_vehicle_label": YES_NO_MAP,
    "do_you_use_guards_at_premise_label": YES_NO_MAP,
    "installed_gps_tracker_in_transit_bags_label": YES_NO_MAP,
    "do_you_use_armed_guards_during_transit_label": YES_NO_MAP,
    "installed_gps_tracker_in_transit_vehicles_label": YES_NO_MAP,

    # --- Records keeping ---
    "records_maintained_in_label": RECORDS_MAP,
    "do_you_keep_detailed_records_of_stock_movements_label": YES_NO_MAP,

    # --- Additional details ---
    "three_piece_rule_label": YES_NO_MAP,
    "the_nearest_police_station_label": POLICE_DISTANCE_MAP,
    "standard_operating_procedure_label": YES_NO_MAP,
    "background_checks_for_all_employees_label": BACKGROUND_CHECK_MAP,
    "how_often_is_the_stock_check_carried_out_label": STOCK_CHECK_MAP,

    # --- Add-on coverage ---
    "director_house_question_label": YES_NO_MAP,
    "director_house_question_cctv_label": YES_NO_MAP,
    "director_house_question_safe_label": YES_NO_MAP,
    "director_house_question_burglar_system_label": YES_NO_MAP,
    "fidelity_guarantee_insurance_add_coverage_label": YES_NO_MAP,
    "exhibtion_coverage_question_label": YES_NO_MAP,
    "outward_entrustment_question_label": YES_NO_MAP,
    "international_coverage_question_label": YES_NO_MAP,
    "exhibition_insurance_question_label": EXHIBITION_INSURANCE_MAP,
    "destination_airport_label": DESTINATION_AIRPORT_MAP,

    # --- Claim history ---
    "claim_history_label": CLAIM_STATUS_MAP,

    # --- Shop lifting ---
    "shop_lifting_label": YES_NO_MAP,
}


# ===========================================================================
# HUMAN-READABLE FIELD LABELS (section -> field -> display name)
# ===========================================================================

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
    "door_access": {
        "door_access_label": "Door Access Type",
        "door_access_others_label": "Door Access (Other)",
        "others_label": "Others",
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
    "transit_and_gaurds": {
        "usage_of_jaguar_transit_label": "Jaguar Transit Used",
        "do_you_use_armoured_vehicle_label": "Armoured Vehicle Used",
        "do_you_use_guards_at_premise_label": "Guards at Premise",
        "installed_gps_tracker_in_transit_bags_label": "GPS Tracker in Transit Bags",
        "do_you_use_armed_guards_during_transit_label": "Armed Guards During Transit",
        "installed_gps_tracker_in_transit_vehicles_label": "GPS Tracker in Transit Vehicles"
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
    "claim_history": {
        "claim_history_label": "Claim History Status",
        "description_label": "Claim Description",
        "year_of_claim_label": "Year of Claim",
        "amount_of_claim_label": "Amount of Claim"
    },
    "premise_sub_limit": {
        "maximum_value_kept_as_display_at_during_business_hours_aw_label": "Max Display Value (Business Hours) - AW",
        "maximum_value_kept_as_display_at_during_business_hours_1ar_label": "Max Display Value (Business Hours) - 1AR",
        "maximum_value_kept_as_display_at_during_business_hours_1pd_label": "Max Display Value (Business Hours) - 1PD",
        "maximum_value_kept_as_display_at_during_business_hours_aws_label": "Max Display Value (Business Hours) - AWS",
        "maximum_value_kept_as_display_at_during_after_business_hours_aw_label": "Max Display Value (After Business Hours) - AW",
        "maximum_value_kept_as_display_at_during_after_business_hours_1ar_label": "Max Display Value (After Business Hours) - 1AR",
        "maximum_value_kept_as_display_at_during_after_business_hours_1pd_label": "Max Display Value (After Business Hours) - 1PD",
        "maximum_value_kept_as_display_at_during_after_business_hours_aws_label": "Max Display Value (After Business Hours) - AWS"
    },
    "shop_lifting": {
        "shop_lifting_label": "Shop Lifting Coverage"
    },
    "summary_coverage_values": {
        "overseas_carrying_label": "Overseas Carrying",
        "sum_assured_limit_label": "Sum Assured Limit",
        "public_exhibitions_label": "Public Exhibitions",
        "destination_airport_label": "Destination Airport",
        "risk_location_address_label": "Risk Location Address",
        "authorized_company_name_label": "Authorized Company Name",
        "director_house_coverage_label": "Director House Coverage",
        "director_house_question_label": "Director House Question",
        "exhibtion_coverage_question_label": "Exhibition Coverage Question",
        "director_house_question_cctv_label": "Director House CCTV",
        "director_house_question_safe_label": "Director House Safe",
        "fidelity_guarantee_insurance_label": "Fidelity Guarantee Insurance",
        "outward_entrustment_question_label": "Outward Entrustment Question",
        "exhibition_insurance_question_label": "Exhibition Insurance Question",
        "fidelity_guarantee_total_staff_label": "Fidelity Guarantee Total Staff",
        "international_coverage_question_label": "International Coverage Question",
        "director_house_question_burglar_system_label": "Director House Burglar System",
        "fidelity_guarantee_insurance_add_coverage_label": "Fidelity Guarantee Add Coverage"
    },
    "sum_assured": {
        "property_label": "Property Type",
        "risk_address_label": "Risk Address",
        "nature_of_business_label": "Nature of Business",
        "maximum_stock_in_premises_label": "Maximum Stock in Premises (MYR)",
        "value_of_stock_out_of_safe_label": "Value of Stock Outside Safe (MYR)",
        "maximum_stock_during_transit_label": "Maximum Stock During Transit (MYR)",
        "maximum_cash_in_premises_label": "Maximum Cash in Premises (MYR)",
        "maximum_foreign_currency_label": "Maximum Foreign Currency (MYR)",
        "value_of_cash_in_premise_label": "Value of Cash in Premises (MYR)",
        "value_of_pledged_stock_in_premise_label": "Value of Pledged Stock (MYR)",
        "value_of_non_pledged_stock_in_premise_label": "Value of Non-Pledged Stock (MYR)",
        "maximum_stock_foreign_currency_in_premise_label": "Max Foreign Currency in Premises (MYR)"
    },
    "industry_id": {
        "industry_id_label": "Industry"
    },
    "businesstype_id": {
        "businesstype_id_label": "Business Type"
    }
}


# ===========================================================================
# CORE DECODE FUNCTION
# ===========================================================================

def decode_field(field_name: str, value) -> str:
    """
    Decode a single field value using the routing table.

    Strategy:
    1. If field is in PASSTHROUGH_FIELDS -> return as-is
    2. If field is in FIELD_DECODE_TABLE -> use that map
    3. Otherwise return as-is (direct value)

    Args:
        field_name: The exact field name (e.g. "premise_type_label")
        value: The raw coded value (e.g. "001")

    Returns:
        Human-readable decoded string
    """
    if value is None or value == "" or not isinstance(value, (str, int, float, bool)):
        return str(value) if value is not None else ""

    value_str = str(value).strip()

    # Passthrough fields - always return raw value
    if field_name in PASSTHROUGH_FIELDS:
        return value_str

    # Check explicit routing table
    decode_map = FIELD_DECODE_TABLE.get(field_name)
    if decode_map is not None:
        decoded = decode_map.get(value_str)
        if decoded is not None:
            return decoded
        # For business type / industry, show unknown code rather than raw number
        if decode_map in (BUSINESS_TYPE_MAP, INDUSTRY_MAP):
            if value_str.isdigit():
                return f"Unknown ({value_str})"
        # For Yes/No maps with non-standard values, return raw
        return value_str

    # No explicit mapping - return as-is
    return value_str


def decode_record(data, section: str = ""):
    """
    Recursively decode all fields in a record (dict or list).

    Args:
        data: Raw data (dict, list, or primitive)
        section: Section name for context

    Returns:
        Decoded data with human-readable values
    """
    # Handle list of dicts (e.g., safe section)
    if isinstance(data, list):
        return [
            decode_record(item, section) if isinstance(item, dict) else item
            for item in data
        ]

    # Handle non-dict types
    if not isinstance(data, dict):
        return data

    # Handle dict
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
    import json as _json

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
                parsed = _json.loads(val) if isinstance(val, str) else val
                if isinstance(parsed, dict):
                    return decode_record(parsed, col)
                elif isinstance(parsed, list):
                    return [decode_record(item, col) if isinstance(item, dict) else item for item in parsed]
                return parsed
            except (_json.JSONDecodeError, TypeError):
                return val

        df[col] = df[col].apply(parse_and_decode)

    # Decode regular columns
    for col in df.columns:
        if col in json_columns:
            continue

        # Apply field-level decoding
        df[col] = df[col].apply(lambda val: decode_field(col, val) if pd.notna(val) else val)

    return df
