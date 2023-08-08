from configuration import *
from etl import transforms
from etl import ETL

tagging_jobs = [
    {
        "tag_id": '1',
        "title": 'd_category',
        "query": """
            accessor* OR "pollen press" OR "pollinator" OR "steel screen" OR "glass bowl" OR "hand pipe" OR "dab mat" OR "rolling tray" OR "bong" OR "bongs" OR "beaker" OR "dugout" OR "dab tool" OR "glass pipes" OR "steam roller" OR "lighters" OR "ash tray" OR "puffco" OR "grinder" OR "one hitter" OR "chillum" OR "chillums" OR "bubbler" OR "handpipe" OR "nectar straw" OR "doob tube" OR "smell proof pouch" OR "smell proof bag" OR "humidity pack" OR "humidity control" OR "rolling machine" OR "water pipes" OR "quartz banger" OR "carb cap" OR "storage container" OR "steamroller" OR "dab stick" OR "vape pouch" OR "rolling rollbox" OR "downstem" OR "steel screens" OR "cork jar" OR "concentrate tool" OR "waterpipe" OR "dabbing stand" OR "banger pearls" OR "ash catcher" OR "ashcatcher" OR "screwtop jar" OR "odor eliminator" OR "screens brass" OR "brass screens" OR "cleaning tool" OR "cone tube" OR "perc" OR "ashtray" OR "kushkards" OR "kush kards" OR "rolling paper" OR "hemp wrap" OR "mouthpiece" OR "face masks" OR "incense" OR "percolator" OR "straight tube" OR "borosilicate glass" OR "hextatic tube" OR "cotton filters" OR "hemp wick" OR "zong" OR "honeycomb tube" OR "red eye tek" OR "storage case" OR "water pipe" OR "blazy susan" OR "retro glass" OR "red eye glass" OR "pull stem"
                """,
        "tag": 'Accessories'
    },
    {
        "tag_id": '2',
        "title": 'd_category',
        "query": """
            concentrat* OR "caviar" OR "moon rock" OR "moon rocks" OR "moonrock" OR "moonrocks" OR "diamond" OR "diamonds" OR "distillate" OR "distillates" OR "bubble hash" OR "hash" OR "hashish" OR "ice hash" OR "pressed dry sift" OR "dry sift" OR "kief" OR "concentrate" OR "concentrates" OR "crystalline" OR "honey oil" OR "oil extracts" OR "RSO" OR "rick simpson oil" OR "terp jelly" OR "hash rosin" OR "live resin" OR "live rosin" OR "live sugar" OR "resin" OR "rosin" OR "terp slush" OR "terp sugar" OR "terp sauce" OR "badder" OR "batter" OR "briser" OR "budder" OR "shatter" OR "shatters" OR "crumble" OR "honeycomb wax" OR "live wax" OR "sugar wax"
        """,
        "tag": 'Concentrates'
    },
    {
        "tag_id": '3',
        "title": 'd_category',
        "query": """
            "lemonade" OR "cider" OR "beverage" OR "beverages" OR "canned" OR "dissolving" OR "flavored powder" OR "flavoured powder" OR "ginger ale" OR "ginger beer" OR "iced tea" OR "legal tonic" OR "quencher" OR "sachet" OR "sachets" OR "infused soda" OR "sparkling beverage" OR "sparkling water" OR "syrup" OR "tea" OR "tonic"
       """,
        "tag": 'Drinks'
    },
    {
        "tag_id": '4',
        "title": 'd_category',
        "query": """
           edibl* OR "baked treat" OR "baked treats" OR "bakery" OR "brownie" OR "brownie bites" OR "brownie-like" OR "brownies" OR "cocoa powder" OR "cookie" OR "cookie bites" OR "cookies" OR "fresh baked" OR "fresh-baked" OR "infused baked medibles" OR "krispy bar" OR "macaron" OR "marshmallow" OR "marshmallows" OR "muffins" OR "muffin" OR "rice cereal" OR "fudge brownie" OR "brownie square" OR "fudgy brownie" OR "stroopwafel" OR "tea cake" OR "waffle" OR "waffles" OR "apple belts" OR "candies" OR "candy" OR "caramel chew" OR "chews" OR "confection" OR "confectioner" OR "confectionery" OR "cubes" OR "drops" OR "fruit bites" OR "fruit chews" OR "fruit drops" OR "fruit rings" OR "fruit snacks" OR "gum" OR "gumm" OR "gummi" OR "gummie" OR "gummies" OR "gummy" OR "hard candy" OR "lozenge" OR "mints" OR "soft chew" OR "soft chews" OR "sour belts" OR "sour drops" OR "watermelon drops" OR "watermelon rings" OR "watermelon slices" OR "jellies" OR "jelly" OR "belt" OR "marmas" OR "marma" OR "chew" OR "chews" OR "mint" OR "mints" OR "bonbons" OR "bonbon" OR "drops" OR "lollipop" OR "taffies" OR "taffy" OR "whizbang" OR "suckers" OR "sucker" OR "sweets" OR "sweet" OR "pucker" OR "white chocolate" OR "white chocolates" OR "peanut butter cup" OR "choc" OR "chocolate" OR "chocolate bar" OR "chocolate bars" OR "chocolate bites" OR "chocolate square" OR "dark chocolate" OR "milk chocolate bar" OR "mint chocolate" OR "per bar" OR "truffle" OR "truffles" OR "barbecue sauce" OR "bbq sauce" OR "cake mix" OR "chocolate syrup" OR "condiment" OR "condiments" OR "extra virgin" OR "honey stick" OR "hot sauce" OR "olive oil" OR "chamoy" OR "raw honey"
        """,
        "tag": 'Edibles'
    },
    {
        "tag_id": '5',
        "title": 'd_category',
        "query": """
            weed OR flower OR "7 grams" OR "1/4th" OR "1/8th" OR "3.5 grams" OR "3.5g" OR "bud" OR "buds" OR "nugs" OR "cannabis flower" OR "cannabis flowers" OR "pre-ground cannabis" OR "preground cannabis"
        """,
        "tag": 'Flower'
    },
    {
        "tag_id": '6',
        "title": 'd_category',
        "query": """
            preroll* OR "pre-roll" OR "pre-rolls" OR "pre roll" OR "pre rolls"
        """,
        "tag": 'Pre-Rolls'
    },
    {
        "tag_id": '7',
        "title": 'd_category',
        "query": """
            "capsule" OR "capsules" OR "gel caps" OR "gelcaps" OR "soft gels" OR "softgels" OR "sublingual strips" OR "sublinguals" OR "sublingual" OR "oral spray" OR "tincture" OR "tinctures" OR "water tincture"
        """,
        "tag": 'Non-Food Ingestibles'
    },
    {
        "tag_id": '8',
        "title": 'd_category',
        "query": """
           "balm" OR "balms" OR "bath bomb" OR "bath salt" OR "bath salts" OR "bath soak" OR "body cream" OR "body lotion" OR "body oil" OR "bubble bath" OR "cream" OR "face cream" OR "face lotion" OR "body gel" OR "gel pen" OR "body gels" OR "lip balm" OR "lubricant" OR "massage lotion" OR "massage oil" OR "moisturizing" OR "ointment" OR "pain stick" OR "relief stick" OR "roll on" OR "roll-on" OR "salve" OR "scrubs" OR "serum" OR "transdermal" OR "topicals" OR "transdermal balm" OR "transdermal gel pen" OR "transdermal pen" OR "transdermal patches" OR "transdermal patch" OR "absorbed through the skin" OR "skin absorbs"
       """,
        "tag": 'Topicals'
    },
    {
        "tag_id": '9',
        "title": 'd_category',
        "query": """
            vape* OR vaping OR "510 thread" OR "550mah" OR "601 thread" OR "vape battery" OR "pax battery" OR "510 cartridge" OR "cartrdges" OR "cartridge" OR "cartridges" OR "carts" OR "oil cartridge" OR "oil cartridges" OR "pax pod" OR "vape cart" OR "vape cartridge" OR "vape pen cartridge" OR "disposable distillate pen" OR "disposable pens" OR "disposable hybrid pen" OR "disposable indica pen" OR "disposable pen" OR "disposable thc vape pen" OR "disposable vape pen" OR "disposables" OR "vape pen" OR "vape pens" OR "vape cup" OR "vape cups" OR "vape juice" OR "vaporizer cup" OR "vaporizer cups"
        """,
        "tag": 'Vapes'
    },
    {
        "tag_id": '10',
        "title": 'd_cannabinoid',
        "query": """
            "delta-9" OR "delta-9-thc" OR "delta-9 thc" OR "tetrahydrocannabinol"
        """,
        "tag": 'THC'
    },
    {
        "tag_id": '11',
        "title": 'd_cannabinoid',
        "query": """
            "cannabidiol" OR "cbd" OR "cbda"
        """,
        "tag": 'CBD'
    },
    {
        "tag_id": '12',
        "title": 'd_cannabinoid',
        "query": """
            "delta-8" OR "d8-thc" OR "delta8" OR "delta 8" OR "weed lite " OR "diet weed " OR "Iite weed " OR "cannabis light"
        """,
        "tag": 'Delta-8-THC'
    },
    {
        "tag_id": '13',
        "title": 'd_cannabinoid',
        "query": """
            "tetrahydrocannabinolic acid" OR "thc-a" OR "thc acid" OR "thca"
        """,
        "tag": 'THCa'
    },
    {
        "tag_id": '14',
        "title": 'd_cannabinoid',
        "query": """
            "delta-10" OR "d10-thc" OR "delta10" OR "delta 10"
        """,
        "tag": 'Delta-10-THC'
    },
    {
        "tag_id": '15',
        "title": 'd_cannabinoid',
        "query": """
            cannabichromene OR ("cbc" AND cannab*)
        """,
        "tag": 'CBC'
    },
    {
        "tag_id": '16',
        "title": 'd_cannabinoid',
        "query": """
            "cannabigerol" OR ("cbg" AND cannab*)
        """,
        "tag": 'CBG'
    },
    {
        "tag_id": '17',
        "title": 'd_cannabinoid',
        "query": """
            cannabinol OR ("cbn" AND cannab*)
        """,
        "tag": 'CBN'
    },
    {
        "tag_id": '18',
        "title": 'd_cannabinoid',
        "query": """
             ("cbt" and "cannab*") OR "cannabitriol" NOT (exam OR therapy OR mental OR behavior OR therapist OR councillor OR councilling OR psychiatrist OR psychologist)
        """,
        "tag": 'CBT'
    },
    {
        "tag_id": '19',
        "title": 'd_cannabinoid',
        "query": """
            "thc o" OR "thc o acetate" OR "thc acetate"
        """,
        "tag": 'THC-O'
    },
    {
        "tag_id": '20',
        "title": 'd_condition',
        "query": """
            "cannabis use disorder" OR (("cud" OR "c.u.d.") AND cannab*)
        """,
        "tag": 'Cannabis Use Disorder'
    },
    {
        "tag_id": '21',
        "title": 'd_condition',
        "query": """
            "cannabis hyperemesis syndrome" OR "cannabinoid hyperemesis syndrome" OR ("chs" AND cannab*) OR ("c.h.s." AND cannab*)
        """,
        "tag": 'Cannabis Hyperemesis Syndrome'
    },
    {
        "tag_id": '22',
        "title": 'd_condition',
        "query": """
            "acne" OR "pimples" OR "zits" OR "pimple" OR "whiteheads" OR "whitehead" OR "blackheads" OR "blackhead" OR "clogged pores"
        """,
        "tag": 'Acne'
    },
    {
        "tag_id": '23',
        "title": 'd_condition',
        "query": """
            "addiction" OR "marijuana addiction" OR "addict" OR "addictions" OR "compulsive behavior" OR "drug addiction" OR "withdrawal symptoms"
        """,
        "tag": 'Addiction'
    },
    {
        "tag_id": '24',
        "title": 'd_condition',
        "query": """
           "hyperactivity" OR "adhd" OR "attention deficit"
        """,
        "tag": 'ADHD'
    },
    {
        "tag_id": '25',
        "title": 'd_condition',
        "query": """
            "allergy" OR "allergies" OR "hay fever" OR "allergic" OR "anaphylaxis" OR "anaphylactic" OR "anti-histamine" OR "antihistamine" OR "allergist" OR "epipen"
        """,
        "tag": 'Allergies'
    },
    {
        "tag_id": '26',
        "title": 'd_condition',
        "query": """
            "alzheimer's disease" OR "alzheimers" OR "dementia"
        """,
        "tag": "Alzheimer's Disease"
    },
    {
        "tag_id": '27',
        "title": 'd_condition',
        "query": """
            "amyotrophic lateral sclerosis" OR "lou gehrig's disease" OR "lou gehrigs"
        """,
        "tag": "Amyotrophic Lateral Sclerosis (ALS)"
    },
    {
        "tag_id": '28',
        "title": 'd_condition',
        "query": """
            "anxiety" OR "anxiety attack" OR "anxiousness" OR "panic attack"
        """,
        "tag": "Anxiety"
    },
    {
        "tag_id": '29',
        "title": 'd_condition',
        "query": """
            "osteoperosis" OR "arthritis" OR ''osteoarthiritis''
        """,
        "tag": "Arthritis"
    },
    {
        "tag_id": '30',
        "title": 'd_condition',
        "query": """
            "autism" OR "autistic" OR "on the spectrum" OR "asd" OR "autism spectrum disorder"
        """,
        "tag": "Autism"
    },
    {
        "tag_id": '31',
        "title": 'd_condition',
        "query": """
            "cancer" OR "leukemia" OR "carcinoma" OR "sarcoma" OR "lymphoma" OR "malignant tumor" OR "osteosarcoma" OR "melanoma" OR "squamous" OR "metastatic" OR "chemo" OR "chemotherapy" OR "radiation therapy" OR "oncology" OR "oncologist"
        """,
        "tag": "Cancer"
    },
    {
        "tag_id": '32',
        "title": 'd_condition',
        "query": """
            "heart disease" OR "hypertension" OR "arrythmia" OR "aorta disease" OR "congential heart disease" OR "coronary artery" OR "deep vein thrombosis" OR "pulmonary embolism" OR "cardiomyopathy" OR "heart valve" OR "pericardial disease" OR "peripheral vascular" OR "vascular disease" OR "heart attack" OR "chest pain" OR "cardiovascular disease" OR "high blood pressure" OR "high cholesterol"
        """,
        "tag": "Cardiovascular diseases"
    },
    {
        "tag_id": '33',
        "title": 'd_condition',
        "query": """
            "celiac" OR "gluten sensitivity" OR "gluten intolerance" OR "celiac"
        """,
        "tag": "Celiac Disease"
    },
    {
        "tag_id": '34',
        "title": 'd_condition',
        "query": """
            "common cold" OR "flu" OR "influenza" OR "stomach bug" OR "stomach flu" OR "gastroenteritis" OR "sinus infection" OR "sinus pressure" OR "upper respiratory" OR "strep throat" OR "cough"
        """,
        "tag": "Common Illnesses"
    },
    {
        "tag_id": '35',
        "title": 'd_condition',
        "query": """
            "chrons" OR "chron's"
        """,
        "tag": "Crohn's"
    },
    {
        "tag_id": '36',
        "title": 'd_condition',
        "query": """
            "depression" OR "depressed" OR "suicide" OR "suicidal"
        """,
        "tag": "Depression"
    },
    {
        "tag_id": '37',
        "title": 'd_condition',
        "query": """
            "neuropathy" OR "diabetes" OR "insulin" OR "blood sugar" OR "neuropathy" OR "blood glucose" OR "diabetic"
        """,
        "tag": "Diabetes"
    },
    {
        "tag_id": '38',
        "title": 'd_condition',
        "query": """
            "indigestion" OR "peptic ulcer disease" OR "stomach ulcer" OR "stomach ulcers" OR "gastroesophageal reflux" OR "gastritis" OR "hemorrhoids" OR "peptic ulcer" OR "endoscopy" OR "crohns disease" OR "stomachache" OR "stomach ache" OR "upset stomach" OR "constipation" OR "diarrhea" OR "gastroesophageal reflux disease"
        """,
        "tag": "Digestive Conditions"
    },
    {
        "tag_id": '39',
        "title": 'd_condition',
        "query": """
            "eczema" OR "atopic dermatitis" OR "exzema" OR "itchy skin"
        """,
        "tag": "Eczema"
    },
    {
        "tag_id": '40',
        "title": 'd_condition',
        "query": """
            "hyperthyroidism" OR "hypogonadism" OR "hypothyroidism"
        """,
        "tag": "Endocrine Disorders"
    },
    {
        "tag_id": '41',
        "title": 'd_condition',
        "query": """
            "epilepsy" OR "epileptic" OR "seizure" OR "seizures" OR "petit mal" OR "dialeptic" OR "anticonvulsant" OR "epileptogenic" OR "grand-mal" OR "grand mal" OR "petit-mal" OR "lesionectomy" OR "dravet syndrom" OR "lennox-gastaut syndrome" OR "lennox gastaut syndrome"
        """,
        "tag": "Epilepsy"
    },
    {
        "tag_id": '42',
        "title": 'd_condition',
        "query": """
            "glaucoma"
        """,
        "tag": "Glaucoma"
    },
    {
        "tag_id": '43',
        "title": 'd_condition',
        "query": """
            "headache" OR "headaches" OR "head-ache" OR "head ache"
        """,
        "tag": "Headaches"
    },
    {
        "tag_id": '44',
        "title": 'd_condition',
        "query": """
            "heartburn" OR "heart burn" OR "acid reflux"
        """,
        "tag": "Hearburn & Acid Reflux"
    },
    {
        "tag_id": '45',
        "title": 'd_condition',
        "query": """
            "hip replacement" OR "knee replacement" OR "joint replacement"
        """,
        "tag": "Hip, Knee, or joint surgery"
    },
    {
        "tag_id": '46',
        "title": 'd_condition',
        "query": """
            "hiv" OR "aids"
        """,
        "tag": "HIV/AIDS"
    },
    {
        "tag_id": '47',
        "title": 'd_condition',
        "query": """
            "irritable bowel syndrome" OR "ibs"
        """,
        "tag": "IBS"
    },
    {
        "tag_id": '48',
        "title": 'd_condition',
        "query": """
           "joint pain" OR "joint inflammation" OR "knee pain" OR "elbow pain" OR "knuckle pain"
       """,
        "tag": "Joint Pain"
    },
    {
        "tag_id": '49',
        "title": 'd_condition',
        "query": """
            "menstrual" OR "menstruation" OR "period pain"
        """,
        "tag": "Menstruation"
    },
    {
        "tag_id": '50',
        "title": 'd_condition',
        "query": """
            "migraine" OR "migraines" OR "cluster headache" OR "migraine pain"
        """,
        "tag": "Migraines"
    },
    {
        "tag_id": '51',
        "title": 'd_condition',
        "query": """
            "multiple sclerosis" OR "disseminated sclerosis" OR "encephalomyelitis disseminata"
        """,
        "tag": "Multiple Sclerosis"
    },
    {
        "tag_id": '52',
        "title": 'd_condition',
        "query": """
            "obsessive-compulsive disorder" OR "ocd"
        """,
        "tag": "OCD"
    },
    {
        "tag_id": '53',
        "title": 'd_condition',
        "query": """
            "overactive bladder" OR "incontinence"
        """,
        "tag": "Overactive Bladder"
    },
    {
        "tag_id": '54',
        "title": 'd_condition',
        "query": """
            "parkinson's disease" OR "parkinsons" OR "parkinson's"
        """,
        "tag": "Parkinson's Disease"
    },
    {
        "tag_id": '55',
        "title": 'd_condition',
        "query": """
            "pregnant" OR "pregnancy"
        """,
        "tag": "Pregnancy"
    },
    {
        "tag_id": '56',
        "title": 'd_condition',
        "query": """
            "psoriasis" OR "scaly patches" OR "psoriatic" OR "psoriatic arthritis"
        """,
        "tag": "Psoriasis"
    },
    {
        "tag_id": '57',
        "title": 'd_condition',
        "query": """
            "asthma" OR "bronchitis" OR "pulmonary disease"
        """,
        "tag": "Pulmonary"
    },
    {
        "tag_id": '58',
        "title": 'd_condition',
        "query": """
            "schizophrenia" OR "schizo"
        """,
        "tag": "Schizophrenia"
    },
    {
        "tag_id": '59',
        "title": 'd_condition',
        "query": """
            "severe pain" OR "chronic pain" OR "daily pain" OR "back pain"
        """,
        "tag": "Severe or Chronic Pain"
    },
    {
        "tag_id": '60',
        "title": 'd_condition',
        "query": """
            "erectile dysfunction" OR "infertility"
        """,
        "tag": "Sexual Health"
    },
    {
        "tag_id": '61',
        "title": 'd_condition',
        "query": """
            "narcolepsy" OR "insomnia" OR "insomniac" OR "can't fall asleep" OR "stay asleep" OR "sleeplessness" OR "falling asleep" OR "staying asleep" OR "sleep aid" OR "sleep apnea" OR "restless leg syndrome" OR "sleep-wake" OR "sleep cycle" OR "sleep patterns" OR "trouble sleeping"
        """,
        "tag": "Sleep Disorders"
    },
    {
        "tag_id": '62',
        "title": 'd_condition',
        "query": """
            "concussion" OR "chronic traumatic encephalopathy" OR "cte" OR "spinal cord disease" OR "spinal cord injury" OR "brain injury" OR "traumatic brain injury" OR "post-concussion syndrome"
        """,
        "tag": "Spinal Cord & Brain Injury"
    },
    {
        "tag_id": '63',
        "title": 'd_condition',
        "query": """
            "tourette's syndrome" OR "tourettes" OR "tourette's"
        """,
        "tag": "Tourette's Syndrome"
    },
    {
        "tag_id": '64',
        "title": 'd_condition',
        "query": """
            "urinary tract infection" OR "uti"
        """,
        "tag": "UTI"
    },
    {
        "tag_id": '65',
        "title": 'd_condition',
        "query": """
            "uterus" OR "women's health" OR "menopause"
        """,
        "tag": "Women's Health"
    },
    {
        "tag_id": '66',
        "title": 'd_condition',
        "query": """
            "dehydration" OR "dehydrated" OR "dehydrate"
        """,
        "tag": "Dehydration"
    },
    {
        "tag_id": '67',
        "title": 'd_condition',
        "query": """
            "hallucinating" OR "hallucinate" OR "hallucination" OR "hallucinations"
        """,
        "tag": "Hallucination"
    },
    {
        "tag_id": '68',
        "title": 'd_condition',
        "query": """
            "nausea" OR ''nauseous'' OR ''nauseate'' OR ''nauseated'' OR ''quesy'' OR "greening out'' OR ''green out''
        """,
        "tag": "Nausea"
    },
    {
        "tag_id": '69',
        "title": 'd_condition',
        "query": """
            "drowsy" OR "drowsiness"
        """,
        "tag": "Drowsiness"
    },
    {
        "tag_id": '70',
        "title": 'd_condition',
        "query": """
            "coronavirus" OR "covid19" OR "covid-19" OR "covid"
        """,
        "tag": "COVID-19"
    },
    {
        "tag_id": '71',
        "title": 'd_condition',
        "query": """
            "stress" OR "stressed" OR "ptsd" OR "post-traumatic stress" OR "post traumatic stress" OR "stage fright"
        """,
        "tag": "Stress"
    },
    {
        "tag_id": '72',
        "title": 'd_condition',
        "query": """
            "vomit" OR "vomiting" OR "vomited" OR "puke" OR "puked"
        """,
        "tag": "Vomiting"
    },
    {
        "tag_id": '73',
        "title": 'd_condition',
        "query": """
            "tinnitus"
        """,
        "tag": "Tinnitus"
    },
    {
        "tag_id": '74',
        "title": 'd_condition',
        "query": """
            fibromyalgia
        """,
        "tag": "Fibromyalgia"
    },
    {
        "tag_id": '75',
        "title": 'd_condition',
        "query": """
            psychosis
        """,
        "tag": "Psychosis"
    },
    {
        "tag_id": '76',
        "title": 'd_condition',
        "query": """
            "sweating" OR "sweaty" OR "sweated"
        """,
        "tag": "Sweating"
    },
    {
        "tag_id": '77',
        "title": 'd_condition',
        "query": """
            amnesia
        """,
        "tag": "Amnesia"
    },
    {
        "tag_id": '78',
        "title": 'd_condition',
        "query": """
            "had a stroke" OR "have a stroke" OR "having a stroke"
        """,
        "tag": "Stroke"
    },
    {
        "tag_id": '79',
        "title": 'd_condition',
        "query": """
            "alchoholic" OR "alchoholism" OR "alchoholics" OR "alchohol dependency"
        """,
        "tag": "Alchoholism"
    },
]

BRIDGE_TABLE_DEFINITIONS = [
    {
        'bridge_table_name': 'b_tags',
        'dim_table_name': 'd_tags',
        'id_col_name': 'tag_id',
        'integrity_checks': [],
        'key_name': 'tag',
        'map_alternate_names': False,
        'master_table_cols': ['text'],
        'schema': [
            {'name': 'unique_id', 'type': 'STRING'},
            {'name': 'tag_id', 'type': 'STRING'},
            {'name': 'matched_terms', 'type': 'STRING'}
        ],
        'secondary_column': None,
        'transform': transforms.query_tagger(tagging_jobs),
    }
]

MASTER_TABLE_DEFINITION = [
    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'mention_title',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'mention_title',
        'transform': None
    },
    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'text',  # mention_body
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'text',
        'transform': None
    },
    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'mention_type',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'mention_type',
        'transform': None
    },
    {
        'bq_type': 'DATETIME',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'whoosh_index_date',  # processing_date
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'processing_date',
        'transform': None
    },
    {
        'bq_type': 'DATETIME',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'created_datetime',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'created_datetime',
        'transform': None
    },
    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'mention_url',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'mention_url',
        'transform': None
    },

    {
        'bq_type': 'INTEGER',
        'dtype': 'Int64',
        'encode': False,
        'encode_with': None,
        'input_col': 'likes',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'likes',
        'transform': None
    },

    {
        'bq_type': 'FLOAT',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'dislikes',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'dislikes',
        'transform': None
    },

    {
        'bq_type': 'INTEGER',
        'dtype': 'Int64',
        'encode': False,
        'encode_with': None,
        'input_col': 'replies',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'replies',
        'transform': None
    },

    {
        'bq_type': 'FLOAT',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'shares',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'shares',
        'transform': None
    },

    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'channel',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'channel',
        'transform': None
    },

    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'sentiments',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'sentiment',
        'transform': None
    },

    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'data_source',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'data_source',
        'transform': None
    },

    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'username',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'username',
        'transform': None
    },


    {
        'bq_type': 'STRING',
        'dtype': 'object',
        'encode': False,
        'encode_with': None,
        'input_col': 'id',
        'integrity_checks': [],
        'map_alternate_names': False,
        'output_name': 'id',
        'transform': None
    },

    # user_id not present
]


def main(project, dataset, query):
    etl = ETL(target_bq_dataset=dataset,
              query=query,
              project_id=project,
              bridge_tables=BRIDGE_TABLE_DEFINITIONS,
              master_tables=MASTER_TABLE_DEFINITION,
              tagging_jobs=tagging_jobs,
              )

    etl.run()


if __name__ == '__main__':
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("--project", type=str)
    parser.add_argument("--dataset", type=str)
    parser.add_argument("--query", type=str)
    args = parser.parse_args()

    main(args.project, args.dataset, args.query)
