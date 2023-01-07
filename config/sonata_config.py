from pyspark.sql.types import *
import os

Log_path = os.getcwd()
Log_format = '-->%(asctime)s - %(levelname)s - %(name)s - %(message)s'

score_csv = "s3a://kaleido-ds2/kaleido-credit/Sonata/Staging/scores.csv"
template_csv = "2021_12_20_Sonata_mapping_requirements.csv"

branch_csv = "s3a://kaleido-ds2/kaleido-credit/Sonata/Staging/branch.csv"
district_csv = "s3a://kaleido-ds2/kaleido-credit/Sonata/Staging/district.csv"
division_csv = "s3a://kaleido-ds2/kaleido-credit/Sonata/Staging/division.csv"
hub_csv = "s3a://kaleido-ds2/kaleido-credit/Sonata/Staging/hub.csv"
region_csv = "s3a://kaleido-ds2/kaleido-credit/Sonata/Staging/region.csv"

#spark - connecting - node
spark_node = "local[*]" #"spark://ip-172-31-29-252.ap-south-1.compute.internal:7077"  ## 

# s3 credentials
bucket_name = "kaleido-ds2"
aws_access_key = 'AKIA2QF73PNMLXGUEAE7'#'AKIA2QF73PNML34LVKFB' #'AKIA2QF73PNMO63W5R6A'
aws_secret_key = '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' #'dcPmtGG4wazhd81fYDAhXKwvqXjcI0Eg3KGVsPH0' #'c6IO75OzfW9wgJCbtEJ50rx0EVYPyszcWeaZdpkx'
aws_endpoint_region="s3." + 'ap-south-1' + ".amazonaws.com"

#s3 raw file paths
onetime_load_file_path_raw = "kaleido-credit/Sonata/Staging"
incremental_file_path_raw  = "kaleido-credit/Sonata/incremental"

#cassandra credentials
cassandra_host = '3.108.179.144' #'172.31.9.179'#'3.108.179.144'
cassandra_username = 'srikanth'
cassandra_port = 9042
cassandra_password = 'Sr!k@tH@9760'

#master table for partner sonata : contains all the txn details each customer
cassandra_table = 'sonata_customer_actual_transactions_table_v6' #sonata_customer_actual_transactions_table_2
#'sonata_customer_actual_transactions_table' # sonata_customer_transactions_demo_table2 -> contain one-time - load only

#rejected-record-tracker
cassandra_table_rejected ='sonata_customer_rejected_transactions_table_v6' #'sonata_customer_rejected_transactions_table'

#demography-version-records-tracker
cassandra_version_table ='sonata_customer_version_tracker_table_v6' #'sonata_customer_version_tracker_table'
#keyspace-name
cassandra_keyspace = 'partner_customer_loan_db'

#event-logging-table
cassandra_eventlogging_table = 'sonata_customer_event_logging_table_v6' #'sonata_customer_event_logging_table' #'sonata_event_logging_demo'#''#''

#contains the latest instalment for each customer and its loan id
customer_latest_instalment_tracker = 'sonata_customer_instalment_number_tracker_v6' #'sonata_customer_instalment_number_tracker' 

cassandra_recent_txn_table = 'sonata_customer_recent_transaction_detail_v6'


#1) incremental-data-capture-table contains the incremental customer and loan ids , which will be further processed to postgress-db
postgress_process_table = 'sonata_incremental_account_capture_v6'

#2) inactive-accounts-details , where the last row of each customer has pos>0 and principal_due_cum_sum = disbursed_amount these are recorded
postgress_inactive_account_table = 'sonata_inactive_account_capture_v6'


#postgress credentials
postgress_table = 'sonata_risk_mangement_transaction_table_v6'#'sonata_risk_mangement_transaction_table_v6' 
#sonata_risk_mangement_transaction_table_v5
#'sonata_risk_mangement_transaction_table_v1'
#'sonata_risk_mangement_transaction_table_1'
postgress_username = 'srikanth'
postgress_password = 'srikanth'
postgress_host = '172.31.29.252'
postgress_port = '5432'
postgress_database = 'partner_customer_loan_db'



# psql -h 172.31.29.252 -U srikanth -p 5432

# credentials env
# email for my access
#     setup in local .py setuptool
#     setup for notebooks


standard_nine_files_startswith = ['customer_account_details',
 'customer_address',
 'customer_cashflow',
 'customer_info',
 'customer_other_details',
 'loan_disbursement',
 'loan_proposal',
 'loan_sanction',
 'transactions']
                   


IsLiveCustomer_dict = {
                           '1.0':'Client On boarded or Proposal is under process', 
                           '2.0':'Disburement Done' ,
                           '3.0':'Loan Account Closed' , 
                           '6.0':'Loan Account/Proposal Cancelled'
                       }

PovertyStatus_dict = {
                            '113.0':'Moderate Poor ',
                            '114.0':'Non Poor' ,
                            '115.0':'Poor' ,
                            '116.0':'Very Poor'
                        }

HouseStatus_dict = {
                            '170':'Self Owned',
                            '171':'Parent Family Owned',
                            '172':'Owned with Partner',
                            '173':'Rented',
                            '84':'Own',
                            '92':'Govt.Scheme',
                            '93':'Own',
                            '94':'Rent'
                        }

IdType_dict = {
                            '95':'Bank Pass Book',
                            '96':'Domecile certeficate',
                            '98':'Driving License',
                            '99':'PAN CARD',
                            '100':'Ration Card',
                            '101':'Aadhaar Card',
                            '102':'Voters ID Card',
                            '273':'Passport'
                        }

PurposeID_dict = {
                                '0':'Agricultural seed store','3':'Bhusa Shop','4':'Boring Machine','5':'Chara Machine','6':'Cotan','7':'Fertiliser Shop',
                                '12':'Flower Purchase','14':'Ganna machine','15':'Garden','18':'Land Purchase','19':'Machine',
                                '20':'Mala Phool','21':'Mango Garden','23':'Nut selling','24':'Patthar','25':'Pulses Business',
                                '26':'Seeds Shop','27':'Stone Cutting','28':'Straw Store','29':'Thresar Purchases','30':'Water Related',
                                '31':'Wheat and Rice Selling','34':'Animal Business','35':'Bird Purchase','37':'Buggi','38':'Cage Purchase',
                                '39':'Camal pupose','40':'Cow Purchases','42':'Duck','44':'Goat Purchase','45':'Hen Purpose','46':'Horse Purchase',
                                '48':'Meat Shop','49':'Milk Purpose','50':'Other Animal Purchses','51':'Ox Purchase','52':'Pailesar Machine',
                                '53':'Pig Purchase','54':'PoultryForm','55':'Sheep business','57':'balli Shop','59':'Almeera  Work ',
                                '60':'Baalu','62':'Baind Baja','64':'Bamboo Work ','65':'Band shop','67':'Batti chokha','68':'Belt Making',
                                '70':'Biddi business','71':'Biscuits buisness','72':'Boat Purchases','74':'Box Making','75':'Bricks Making ',
                                '76':'Broom  Work','77':'Bulb Work','78':'Business of gamla','79':'Cable And Dish','80':'Caitring Purpose','81':'Candle making',
                                '83':'Carpet Making','84':'Cement Shop','85':'Chat ki dukan','86':'Clinic','87':'Coal work','88':'Coching Center','90':'Cosmetic shop',
                                '91':'Daal Machine','92':'Daal Mot Business','94':'Dibba Making','96':'Disposal Making Machie','97':'Dona Pattal Making ',
                                '98':'Engineering purpose','100':'Finger Chips','108':'Generator  purchese','109':'Glubs making','110':'Gomati',
                                '112':'Handy Craft Making','113':'Hardware','114':'Hozary shop','116':'Irigation Machine','118':'Jewellery Shop',
                                '119':'Jhaadu Making','121':'Kadai Machine','122':'Kalawa selling','123':'Khairat machine','124':'Kite Making',
                                '126':'lifafa production','127':'LPG Gas','128':'Masala Work','132':'Mobile shop','133':'Mustured','134':'Nail Polish Production',
                                '135':'Namkeen Biscuite shop','136':'Narsuri','137':'Net selling','138':'Oil selling','140':'Pailumber','141':'Papad',
                                '142':'Paper bag','143':'Pataka shop','144':'Pen Work','150':'Polytheen Production','151':'Poster Work','152':'Pot Selling',
                                '154':'Printing card machine','155':'Pumpset','156':'Quilt','157':'Rajai gadda shop','158':'Rajgiree','159':'Redymade garments',
                                '160':'Row','161':'Sack Making','163':'Saloon','164':'Salt Work','166':'Sewing and Embroidry ','167':'Soap buisness','168':'Socks Making',
                                '169':'Sootfeny Making','171':'Stone','172':'Stov shop','174':'Sweet Shop','175':'Tailes','177':'Tent House',
                                '179':'Thrad  Reel','180':'Tiffin Work','181':'Tiles machine purchase','184':'Vikram buisness',
                                '186':'Wheet Machine','187':'Zari buisness','188':'Aata Chakki','189':'Agarbatti and dhoop',
                                '190':'Agriculter Work','191':'Air Machine','193':'Asbestus Chaddar','196':'Bag Making',
                                '197':'Bag Shop','199':'Bakery buisness','200':'BALLON buisness','201':'Bandh Rassi',
                                '202':'Bandparty','204':'Bardana selling','205':'Bartan shop','206':'Basket Purchase',
                                '207':'Batasha Gatta Shop','209':'Beauty parlour','210':'Bedseet selling','211':'Bekary Shop',
                                '212':'Belt Selling','213':'Betal Shop','214':'Ram dana','215':'Bhelpoori','220':'Bishatkhana',
                                '221':'Blanket selling','224':'Boot Polish','225':'Bora Shop','227':'Botel Machine','228':'Boutique',
                                '231':'Brekfast Shop','233':'brush','235':'Building Material Store','236':'Bulks Cart Purchases',
                                '237':'Business','238':'Butique','240':'Camera Shop','242':'Canteen Work','243':'Car buisness',
                                '244':'Carpenter','246':'Carrom Bord ','247':'CD and Cassete shop','249':'Chaat Shop',
                                '250':'Chamda purpose','252':'Charkha','253':'Chaumeen shop','254':'Chilly forming','256':'Chooran selling',
                                '257':'Chuna Shop','258':'Chura Machine','259':'Churry selling','260':'Clock Shop','262':'Cloth  buisness','263':'Coconut Selling',
                                '264':'Coldrink','265':'Colour Work','266':'ComputerShop ','267':'Contractor','268':'Cooker  reparing','269':'Cooking Gas',
                                '270':'Cooler work','272':'Coton Fibers','273':'Crokery shop','274':'Cutter Machine','276':'cylinder Shop',
                                '277':'D.J. Band','279':'Dairy Farm','280':'Decoration','281':'Dhaga Making','282':'Dhool','284':'Dish work',
                                '287':'Drum selling','288':'Dryclean shop','292':'Embroidy shop','294':'Engine Purchase and Repairing',
                                '295':'Envelope making','296':'Factory','297':'Farming buisness','298':'Fashion Wear','299':'Fast food shop',
                                '301':'FiberShop','302':'Fish buisness','304':'Flour Machine','305':'Food Buisness','306':'Footwear Shop',
                                '307':'Fruit shop','308':'Furniture Shop','309':'Gairaz','310':'Galla trading','311':'Gamala Selling',
                                '314':'Gas store','315':'Gas welding','317':'Gate Gril','318':'General Store','322':'Ghasai Machine',
                                '323':'Ghughur Bhatty','324':'Gift corner','325':'Glass Shop','326':'Gogals Shop','327':'Gold work',
                                '328':'Goods Business','329':'Grain mill','331':'Hair cutting','332':'Hand pump purchase',
                                '333':'Heeng Selling ','334':'Helmet Purchases','335':'Hojari Work','336':'Hokers',
                                '337':'Homemade Item Trading','338':'Horse Cart Purchases','339':'Hotel','340':'House hold buisness',
                                '341':'Ice Business','342':'Ice Cream Shop','343':'Instrument ','344':'Invertor selling',
                                '345':'Iron Store','346':'Jalee purchase','347':'Jeep Reparing','349':'Jhoola','351':'Junk Dilar',
                                '354':'Kamani Iron','355':'Kard shop','356':'Key Selling','357':'Khaad shop','358':'Khali Shop',
                                '359':'Kharkhana','360':'Khowa shop','361':'Kirana store','362':'kitchen instrument',
                                '364':'Knife machine purchase','365':'Koyla store','366':'kucker repairing',
                                '368':'Laiya Shop','369':'Lamp selling','370':'Leather Sell','371':'Lebaring',
                                '372':'Led Pot','373':'Light house shop','374':'Lock shop ','375':'Loder Purchase',
                                '376':'Loker selling','377':'Londry','378':'Lubricants Shop','379':'Lunch box selling',
                                '381':'Magzeen Shop','382':'Mahua Selling','385':'Meal part','387':'Mechanic',
                                '388':'Medical Store','389':'Met making','391':'mirror','393':'Mochi',
                                '394':'Mojaik machine','395':'Moong phali buisness','397':'MotorCycle ',
                                '398':'Mungfali Petha Shop','399':'Murti making','400':'Music System',
                                '401':'Naal','402':'Neel work','403':'Net purchase','404':'Nursary Plant',
                                '406':'Oil Machine','407':'Onion selling','408':'Optical house',
                                '409':'Ornament store','410':'Other Repairing','411':'Other Trading',
                                '412':'Oxygen Silender Trading','413':'Paint shop','414':'Painting ','415':'Pan Shop','416':'Paneer selling',
                                '419':'PCO Center','420':'Peanuts buisness','421':'Peeko Machine','422':'Perfume shop','423':'Photo Studio',
                                '424':'Photocopy Shop','427':'Pickle buisness','428':'Pico machine','429':'Pipe selling','430':'Pital Work',
                                '431':'Plastick item Selling','432':'Plumber matarial','433':'Polish machine','434':'Polithin Shop',
                                '435':'Polybag selling','436':'Pop work','437':'Pots and Pans','438':'PotatoSelling','440':'Prasad shop',
                                '441':'Press Shop','442':'Printing Work','445':'RadioShop','446':'Rassi selling','448':'Rebone shop',
                                '449':'Refresment Shop','450':'Repairing Shop','451':'Rewadi Work','452':'Rice Meal','454':'Ring and stone',
                                '455':'Road Light','456':'Rope Making','457':'Salesman','459':'Saree selling','460':'Sariya Gitti Balu buisness',
                                '461':'Scooter Repairing','462':'Screen Printing Machine','463':'Seat cover selling','464':'Sakar Making Work',
                                '465':'Setering maiterial','471':'Sleeper Shop','472':'Snaks selling','473':'Snow Shop','474':'Soup Business',
                                '475':'Spectical shop','476':'Spices Marketing','477':'Sports Shop','478':'Spray Machine','479':'Stamp making',
                                '480':'Stationary purpose','481':'Steel selling','484':'Stove cooker reparing','485':'studio','486':'Suit Business',
                                '487':'Supadi Shop','488':'Sweet Box Purchase','490':'Tailor shop','492':'Tea stall','493':'Telivision shop',
                                '494':'Tempo purchase','495':'Tender','497':'Tier repair','499':'Timbar Store','500':'Tin Sope','501':'Tobeco Shop',
                                '502':'Toys shop','503':'Tradors Shop','504':'Transport services','505':'Trolly purchase','506':'TV shop','507':'Typing  Machine ',
                                '508':'Tyre Shop','509':'Umbrella Shop','510':'Upper machine','511':'Van bussiness','512':'Vedio game and CD Shop',
                                '513':'Vegetable forming','514':'vehicle purchase','515':'Vendor','516':'Vessels Selling','517':'Video Audio Shop',
                                '518':'Vishat Khana','519':'Wardana store','520':'Washing Machine buisness','522':'Watch Shop','523':'Water Motar',
                                '524':'Water Pamp Engine','525':'Watermelon  Business','526':'Welding Shop','528':'Wood  BasketBusiness',
                                '529':'Wood Shop','530':'Woolen store','533':'BiCycle Purchase','534':'Bicycle Reparing Shop','535':'Bike Reparing',
                                '537':'Denting Painting','538':'Diesal ','541':'Four Wheel Purchase ','543':'Gerrage','545':'Maruti repairing',
                                '546':'Maruti Van Paurchasing','550':'MotorCycle Reapairing','551':'Oil Grees','555':'Push Cart Purches',
                                '556':'Riksha Purchases','559':'Self Daynuma','560':'Service Station','561':'Taxi purpose','562':'Tire shop',
                                '563':'Tractar Reparing','567':'Vikram Motor','568':'Utesnsil ShoP','570':'Velding Machine','572':'Metal shop',
                                '574':'Laundry','577':'Book Shop','578':'silaak','579':'Bairing','581':'Pattal Shop','584':'Newspaper',
                                '585':'Bolt Machine','586':'Dry Food','588':'Grosary Shop','590':'Jaiket Work','591':'Foodgrain Trading',
                                '592':'Plate Machine','593':'Video Camra','594':'Paper Cutting Machin','595':'Nug Selling','597':'EAT BAHTTAA',
                                '599':'chaklet shop','600':'Compressor Machine','601':'Kanthi Mala','602':'fairi','604':'Chana Mumfali Selling',
                                '605':'Achar Bussiness','608':'Daliya Pupose','609':'Varing work','611':'Chiken Shop','612':'Vegitable Shop',
                                '613':'Atish bajee Making','614':'Mehadi Selling','615':'XRays Shop','616':'Ready Water','617':'Chhola Bhatura ka Thela',
                                '619':'Shoes  Shop','621':'Animal Hair Cutting','625':'keroseen oil Seling','627':'Gobar','628':'Battery shop','629':'Egg Shop',
                                '631':'Tools','632':'Dana Machine','633':'Biriyani Shop','634':'Chakki','636':'Daree Bunai','637':'Cycle shop',
                                '638':'Nashta carner','639':'Marbal Machine','641':'Dosa Edli Shop','642':'Juice Center','643':'Molding Masing',
                                '644':'Chhuni Chokar shop','647':'Faluda ka thela','649':'Burma Machine','650':'Dhan','651':'Number plate',
                                '652':'Khajoor Petha Shope','653':'L.E.D','654':'Awala Selling','657':'walding macen','658':'Enterprijes',
                                '659':'Playi','661':'Ganna Singhada','662':'Picher Pote shop','663':'Aopticuls shop','664':'Tafi Shop',
                                '666':'Mixchar Machine','667':'Goand Production','668':'Collar Mill','669':'Aurkestra','670':'Worsip Product',
                                '671':'Palledari','672':'Coffee Shop','673':'Stamp Shop','677':'Bus Reparing','679':'Board Meking','680':'Chips',
                                '681':'Auto Mobile','682':'Rafu work','683':'Loom','684':'Softwere Sales','685':'Roi machine','686':'Soot Making',
                                '687':'Ghee Business','688':'teflon Cotting','689':'Vechile Reparing','690':'Hording Bord Making','691':'Artificial',
                                '692':'Interlock Machine','693':'Sells Man','694':'Batan Machine','696':'Ceram Board','697':'Tokari','698':'Kharad Machine',
                                '699':'Drill Machine','702':'Har Maal Selling','703':'Hand work Matarial','704':'Gress Business','705':'Amawat selling',
                                '706':'Putti','707':'Mombatti','708':'Patra Balli','709':'Tyer Panchar Shop','710':'Form Shop','711':'Bentax Jwelry',
                                '712':'Silver Business','713':'Dhaba','714':'Electronic shop','715':'Packing work','716':'Draftary','717':'Chemical Shop',
                                '718':'Punchar machine','719':'Mat Shop','720':'Wiring','721':'Statue Making','722':'Pathology','723':'Pipa work',
                                '724':'Weaving','725':'Grass salling','726':'telecom','727':'finishing machine','728':'Plauting','729':'Rabar MOhar',
                                '731':'Vaipar','732':'Cap selling','733':'Dance classes','735':'Dhobi','736':'cartoon','737':'Chini mill','739':'Laathi',
                                '740':'Aquaguard Repairing','743':'control','746':'Mandir','747':'Poshakh Shop','748':'Mosquito net','749':'Gitti',
                                '750':'Piperment','751':'Motor','752':'Silver work paper','753':'Munga Moti','754':'Bhatti','755':'Cock purchase',
                                '756':'Kanta Bant','757':'Scissors Bussiness','758':'powder business','759':'Murga','761':'Padiya','762':'Whole sale',
                                '763':'Bhed','764':'singhada','765':'Elephent','766':'Batakh','767':'blukart','768':'halwai saman','769':'Farwa Work',
                                '773':'Rakhi selling','775':'sabun','776':'Mobile','777':'CENTERING','778':'speiar parts','779':'kusan work',
                                '781':'pooja samagri','783':'jents parler','784':'MATKA Manifakchiring','785':'SWEET MAKING','787':'Barsati',
                                '788':'Solar Light','790':'manihari','793':'Buffalo Purchase','794':'tala chavi','795':'supe','797':'senting',
                                '798':'SolarChulha','799':'LIGHT','800':'Thela','806':'Swing machine','808':'Colculetor','809':'Science work',
                                '811':'Phulki chat','815':'beej bhandar','816':'Confectionary Shop','818':'katai machine','819':'ptthar ghisai machine',
                                '821':'KHATAI','822':'jalebi ki dukan','825':'ghilti machine','826':'baffao','829':'chadu','830':'bhata',
                                '831':'NAL FITING','832':'Grending machine','834':'Freez','836':'vishatkhana','839':'Chaara',
                                '843':'Chola chawal','845':'Charpaie Work','847':'ASHA','848':'ledij parlr','849':'New Toilet Construction',
                                '850':'Tajiya making','851':'Butterfly','853':'kabad purchaging','854':'bangle shop','856':'Deri',
                                '857':'tanddor bhatti','859':'Home Improvement','860':'aysha','862':'leela','863':'Other',
                                '864':'Toilet Repair','865':'Water Connection','866':'Water and Sanitation'
                         }

PurposeGrpID_dict = {
                                '1':'Agriculture',
                                '2':'Animal Husbandry',
                                '3':'Others',
                                '4':'Production', 
                                '5':'Trading',
                                '6':'Transportation', 
                                '343':'Social Infrastructure'
                       }
ProductCategory_dict = {
                                '23':'Northern arc',
                                '15':'Equitas BC',
                                '9':'Sanitation',
                                '12':'IL Utiliti',
                                '6':'Income Generating Loan ',
                                '7':'Utility',
                                '1':'IndusInd Bank',
                                '19':'Covid Fighter',
                                '5':'DCB Bank',
                                '22':'Indusind Sanitation',
                                '17':'vehicle loan_JLG',
                                '11':'IL',
                                '20':'Street Vendor',
                                '14':'SIDBI BC',
                                '8':'HomeImprovement'
                            }

pin_code_prefix ={
                            '396210'  :    'Daman and Diu',
                            '396'     :    'Dadra and Nagar Haveli',
                            '403' 	  :    'Goa',
                            '605' 	  :    'Puducherry',
                            '682' 	  :    'Lakshadweep',
                            '737' 	  :    'Sikkim',
                            '744' 	  :    'Andaman and Nicobar Islands',
                            '11' 	  :    'Delhi',
                            '12' 	  :    'Haryana',
                            '13' 	  :    'Haryana',
                            '14' 	  :    'Punjab',
                            '15' 	  :    'Punjab',
                            '16' 	  :    'Chandigarh',
                            '17' 	  :    'Himachal Pradesh',
                            '18' 	  :    'Jammu and Kashmir, Ladakh',
                            '19' 	  :    'Jammu and Kashmir, Ladakh',
                            '20' 	  :    'Uttar Pradesh, Uttarakhand',
                            '21' 	  :    'Uttar Pradesh, Uttarakhand',
                            '22' 	  :    'Uttar Pradesh, Uttarakhand',
                            '23' 	  :    'Uttar Pradesh, Uttarakhand',
                            '24' 	  :    'Uttar Pradesh, Uttarakhand',
                            '25' 	  :    'Uttar Pradesh, Uttarakhand',
                            '26' 	  :    'Uttar Pradesh, Uttarakhand',
                            '27' 	  :    'Uttar Pradesh, Uttarakhand',
                            '28' 	  :    'Uttar Pradesh, Uttarakhand',
                            '30' 	  :    'Rajasthan',
                            '31' 	  :    'Rajasthan',
                            '32' 	  :    'Rajasthan',
                            '33' 	  :    'Rajasthan',
                            '34' 	  :    'Rajasthan',
                            '36' 	  :    'Gujarat',
                            '37' 	  :    'Gujarat',
                            '38' 	  :    'Gujarat',
                            '39' 	  :    'Gujarat',
                            '40' 	  :    'Maharashtra',
                            '41' 	  :    'Maharashtra',
                            '42' 	  :    'Maharashtra',
                            '43' 	  :    'Maharashtra',
                            '44' 	  :    'Maharashtra',
                            '45' 	  :    'Madhya Pradesh',
                            '46' 	  :    'Madhya Pradesh',
                            '47' 	  :    'Madhya Pradesh',
                            '48' 	  :    'Madhya Pradesh',
                            '49' 	  :    'Chhattisgarh',
                            '50' 	  :    'Telangana',
                            '51' 	  :    'Andhra Pradesh',
                            '52' 	  :    'Andhra Pradesh',
                            '53' 	  :    'Andhra Pradesh',
                            '56' 	  :    'Karnataka',
                            '57' 	  :    'Karnataka',
                            '58' 	  :    'Karnataka',
                            '59' 	  :    'Karnataka',
                            '60' 	  :    'Tamil Nadu',
                            '61' 	  :    'Tamil Nadu',
                            '62' 	  :    'Tamil Nadu',
                            '63' 	  :    'Tamil Nadu',
                            '64' 	  :    'Tamil Nadu',
                            '65' 	  :    'Tamil Nadu',
                            '66' 	  :    'Tamil Nadu',
                            '67' 	  :    'Kerala',
                            '68' 	  :    'Kerala',
                            '69' 	  :    'Kerala',
                            '70' 	  :    'West Bengal',
                            '71' 	  :    'West Bengal',
                            '72' 	  :    'West Bengal',
                            '73' 	  :    'West Bengal',
                            '74' 	  :    'West Bengal',
                            '75' 	  :    'Odisha',
                            '76' 	  :    'Odisha',
                            '77' 	  :    'Odisha',
                            '78' 	  :    'Assam',
                            '790'	  :    'Arunachal Pradesh',
                            '791'	  :    'Arunachal Pradesh',
                            '792'	  :    'Arunachal Pradesh',
                            '793'	  :    'Meghalaya',
                            '794'	  :    'Meghalaya',
                            '795'	  :    'Manipur',
                            '796'	  :    'Mizoram',
                            '797'	  :    'Nagaland',
                            '798'	  :    'Nagaland',
                            '799'	  :    'Tripura',
                            '80' 	  :    'Bihar, Jharkhand',
                            '81' 	  :    'Bihar, Jharkhand',
                            '82' 	  :    'Bihar, Jharkhand',
                            '83' 	  :    'Bihar, Jharkhand',
                            '84' 	  :    'Bihar, Jharkhand',
                            '85' 	  :    'Bihar, Jharkhand',
                            '90' 	  :    'Army Postal Service',
                            '91' 	  :    'Army Postal Service',
                            '92' 	  :    'Army Postal Service',
                            '93' 	  :    'Army Postal Service',
                            '94' 	  :    'Army Postal Service',
                            '95' 	  :    'Army Postal Service',
                            '96' 	  :    'Army Postal Service',
                            '97' 	  :    'Army Postal Service',
                            '98' 	  :    'Army Postal Service',
                            '99' 	  :    'Army Postal Service'
}


mapper = """ select
coalesce(loan_disbursement.CustomerInfoID,
		 Customer_info.CustomerInfoID,
		 customer_cashflow.CustomerInfoID,
		 customer_other_details.CustomerInfoID,
		 customer_address.CustomerInfoID,
		 customer_account_details.CustInfoID, 
		 transactions.CustomerInfoID,
		 loan_sanction.CustomerInfoID,
		 loan_proposal.CustomerInfoID,0) as   customer_id,
coalesce(loan_disbursement.DisbursementID ,  transactions.DisbursementID,0)   as   loan_account_no ,
nvl(transactions.InstallmentId,1)    as   instalment_number , 
'none'    as   kiscore_tier ,
loan_disbursement.DisbursementDate    as   disbursement_date , 
loan_disbursement.CreatedDate    as   account_creation_date , 
loan_disbursement.DisbursementID    as   account_id , 
loan_disbursement.LoanType    as   account_type , 
loan_disbursement.ActualPaidDate    as   actual_close_date , 
customer_address.ResProofId    as   address_id , 
'loan_application'    as   address_id_type , 
customer_address.CreatedDate    as   address_report_date , 
customer_address.UpdatedDate    as   address_updated_date , 
NULL    as   address_version , 
customer_address.Address1    as   address1 , 
customer_address.Address2    as   address2 , 
customer_other_details.AgricultureIncome    as   agriculture_income , 
customer_other_details.AnimalValue    as   animal_value , 
customer_other_details.AnnualIncome    as   annual_income , 
customer_other_details.CustomerInfoId    as   annual_income_id , 
 'customer_level'    as   annual_income_id_type , 
customer_other_details.UpdatedDate    as   annual_income_report_date , 
customer_other_details.UpdatedDate    as   annual_income_updated_date , 
NULL    as   annual_income_version , 
customer_other_details.AssetValue    as   asset_value , 
customer_other_details.AccountHolderName    as   bank_acc_name , 
customer_other_details.BankBranchName    as   bank_branch_name , 
NULL    as   bank_details , 
customer_other_details.BankName    as   bank_name , 
NULL    as   block , 
loan_disbursement.BranchID    as   branch_id , 
loan_disbursement.DisbursementID    as   branch_id_id , 
loan_disbursement.DisbursementDate    as   branch_id_report_date , 
'loan_account_number'    as   branch_id_type , 
loan_disbursement.DisbursementDate    as   branch_id_updated_date , 
NULL    as   branch_id_version , 
branch.branch_name    as   branch_name , 
NULL    as   cable_tv_connection , 
customer_cashflow.Agriculture    as   cashflow_Agriculture , 
customer_cashflow.BankCloserDate    as   cashflow_BankCloserDate , 
customer_cashflow.BankLiabilities    as   cashflow_BankLiabilities , 
customer_cashflow.BankLoanAmount    as   cashflow_BankLoanAmount , 
customer_cashflow.BankTerm    as   cashflow_BankTerm , 
customer_cashflow.BusinessExpenseOthers    as   cashflow_BusinessExpenseOthers , 
customer_cashflow.BusinessTelephone    as   cashflow_BusinessTelephone , 
customer_cashflow.BusinessTransportation    as   cashflow_BusinessTransportation , 
customer_cashflow.CreatedDate    as   cashflow_CreatedDate , 
customer_cashflow.DailyEarnings    as   cashflow_DailyEarnings , 
customer_cashflow.Education    as   cashflow_Education , 
customer_cashflow.EquipementRent    as   cashflow_EquipementRent , 
customer_cashflow.FinanceCompanyCloserDate    as   cashflow_FinanceCompanyCloserDate , 
customer_cashflow.FinanceCompanyLiabilities    as   cashflow_FinanceCompanyLiabilities , 
customer_cashflow.FinanceCompanyLoanAmt    as   cashflow_FinanceCompanyLoanAmt , 
customer_cashflow.FinanceCompanyTerm    as   cashflow_FinanceCompanyTerm , 
customer_cashflow.FirstQuarterAvgStock    as   cashflow_FirstQuarterAvgStock , 
customer_cashflow.Fooding    as   cashflow_Fooding , 
customer_cashflow.FourthQuarterAvgStock    as   cashflow_FourthQuarterAvgStock , 
customer_cashflow.FurnitureAssets    as   cashflow_FurnitureAssets , 
customer_cashflow.HouseholdAppliances    as   cashflow_HouseholdAppliances , 
customer_cashflow.HouseProperty    as   cashflow_HouseProperty , 
customer_cashflow.IncrExpense    as   cashflow_IncrExpense , 
customer_cashflow.IncrIncome    as   cashflow_IncrIncome , 
customer_cashflow.InsurancePremium    as   cashflow_InsurancePremium , 
customer_cashflow.InsurancePremiumTerm    as   cashflow_InsurancePremiumTerm , 
customer_cashflow.Labour    as   cashflow_Labour , 
customer_cashflow.Liabilities    as   cashflow_Liabilities , 
customer_cashflow.LiveStocks    as   cashflow_LiveStocks , 
customer_cashflow.LivestocksHouseHoldAssets    as   cashflow_LivestocksHouseHoldAssets , 
customer_cashflow.LoanFrmOthersCloserDate    as   cashflow_LoanFrmOthersCloserDate , 
customer_cashflow.LoanFrmOthersLoanAmt    as   cashflow_LoanFrmOthersLoanAmt , 
customer_cashflow.LoanFrmOthersTerm    as   cashflow_LoanFrmOthersTerm , 
customer_cashflow.LoanFromOthers    as   cashflow_LoanFromOthers , 
customer_cashflow.Medical    as   cashflow_Medical , 
customer_cashflow.NetBusinessIncome    as   cashflow_NetBusinessIncome , 
customer_cashflow.NetHouseholdincome    as   cashflow_NetHouseholdincome , 
customer_cashflow.NetIncrIncome    as   cashflow_NetIncrIncome , 
customer_cashflow.NetTotalIncome    as   cashflow_NetTotalIncome , 
customer_cashflow.Other    as   cashflow_Other , 
customer_cashflow.OtherExpenses    as   cashflow_OtherExpenses , 
customer_cashflow.Pension    as   cashflow_Pension , 
customer_cashflow.PlantAssets    as   cashflow_PlantAssets , 
customer_cashflow.PresentValuestock    as   cashflow_PresentValuestock , 
customer_cashflow.ProcessedDate    as   cashflow_ProcessedDate , 
customer_cashflow.Purchage    as   cashflow_Purchage , 
customer_cashflow.RdTerm    as   cashflow_RdTerm , 
customer_cashflow.Rent    as   cashflow_Rent , 
customer_cashflow.RentalIncome    as   cashflow_RentalIncome , 
customer_cashflow.Salary    as   cashflow_Salary , 
customer_cashflow.SecondQuarterAvgStock    as   cashflow_SecondQuarterAvgStock , 
customer_cashflow.ShopAssets    as   cashflow_ShopAssets , 
customer_cashflow.SipTerm    as   cashflow_SipTerm , 
customer_cashflow.StaffId    as   cashflow_StaffId , 
customer_cashflow.Telephone    as   cashflow_Telephone , 
customer_cashflow.ThirdQuarterAvgStock    as   cashflow_ThirdQuarterAvgStock , 
customer_cashflow.TotalAssets    as   cashflow_TotalAssets , 
customer_cashflow.TotalBusinessAssets    as   cashflow_TotalBusinessAssets , 
customer_cashflow.TotalBusinessExpenses    as   cashflow_TotalBusinessExpenses , 
customer_cashflow.TotalBusinessIncome    as   cashflow_TotalBusinessIncome , 
customer_cashflow.TotalHouseholdAssets    as   cashflow_TotalHouseholdAssets , 
customer_cashflow.TotalHouseholdExpenses    as   cashflow_TotalHouseholdExpenses , 
customer_cashflow.TotalHouseHoldIncome    as   cashflow_TotalHouseHoldIncome , 
customer_cashflow.TotalIncomeandExpenses    as   cashflow_TotalIncomeandExpenses , 
customer_cashflow.TotalLiabilitiesandAssets    as   cashflow_TotalLiabilitiesandAssets , 
customer_cashflow.TotalMonthlyAvgSale    as   cashflow_TotalMonthlyAvgSale , 
customer_cashflow.TotalSales    as   cashflow_TotalSales , 
customer_cashflow.Transportation    as   cashflow_Transportation , 
customer_cashflow.UpdatedDate    as   cashflow_UpdatedDate , 
customer_cashflow.Water    as   cashflow_Water , 
loan_disbursement.CenterID    as   center_id , 
loan_disbursement.DisbursementID    as   center_id_id , 
'loan_account_number'    as   center_id_id_type , 
loan_disbursement.DisbursementDate    as   center_id_report_date , 
loan_disbursement.DisbursementDate    as   center_id_updated_date , 
NULL    as   center_id_version , 
NULL    as   chit_fund_savings , 
NULL    as   cluster , 
NULL    as   conf_balance , 
'India'    as   country , 
current_date()+1   as   creation_date , 
NULL    as   customer_language , 
NULL    as   debit_amount , 
NULL    as   debit_date , 
customer_address.UpdatedDate    as   demography_update_date , 
NULL    as   demography_version , 
loan_disbursement.DisbursedAmt    as   disbursed_amount , 
loan_sanction.DisburementStatus    as   disbursement_status , 
Customer_info.districtID    as   district_id , 
district.district_name    as   district_name , 
customer_info.DivisionId    as   division , 
division.division_name as division_name,
customer_other_details.DOB    as   dob , 
customer_other_details.CustomerInfoId    as   dob_id , 
 'customer_level'    as   dob_id_type , 
customer_other_details.CreatedDate    as   dob_report_date , 
customer_other_details.UpdatedDate    as   dob_updated_date , 
NULL    as   dob_version , 
case when  customer_cashflow.Electricity >0 then True else False end    as   eletricity_at_home , 
loan_disbursement.ExpectPaidDate    as   expected_close_date , 
NULL    as   expected_disbursement_date , 
nvl(loan_sanction.FirstInstallmentDate,transactions.ScheduleDate)    as   first_instalment_date , 
NULL    as   fridge , 
loan_disbursement.FunderID    as   funder_name , 
NULL    as   funder_type , 
customer_other_details.Sex    as   gender , 
NULL    as   gold_savings , 
NULL    as   group_code , 
NULL    as   group_formation_date , 
loan_proposal.GroupID    as   group_id , 
NULL    as   group_subvillage_code , 
NULL    as   health_insurance , 
customer_account_details.HomeBranchID    as   home_branch_id , 
NULL    as   house_instalment_amount , 
customer_other_details.HouseStatus    as   house_ownership , 
NULL    as   house_type , 
Customer_info.hubID    as   hub_id , 
hub.hub_name    as   hub_name , 
 'customer_level'    as   hub_id_id_type , 
Customer_info.CreatedDate    as   hub_id_report_date , 
Customer_info.UpdatedDate    as   hub_id_updated_date , 
NULL    as   hub_id_version , 
NULL    as   ideal_balance , 
customer_other_details.IFSCCode    as   ifsc_code , 
NULL    as   income_details , 
NULL    as   index_score , 
loan_disbursement.TotalInstallment    as   instalment_amount , 
NULL    as   insured_amount , 
transactions.InterestAmt    as   interest_amount , 
transactions.InterestCollected as interest_collected ,
NULL    as   interest_rate , 
NULL    as   internet_usage_on_mobile , 
NULL    as   kaliedogoals_goal_target_amount , 
NULL    as   kaliedogoals_savings_amount , 
loan_disbursement.score    as   kiscore_probabbility , 
NULL    as   landmark , 
max(transactions.CollectedDate) OVER (PARTITION BY transactions.CustomerInfoId,transactions.AccountId)    as   last_paid_date , 
max(transactions.ScheduleDate) OVER (PARTITION BY transactions.CustomerInfoId,transactions.AccountId)    as   last_schedule_date , 
customer_info.Latitude    as   latitude , 
NULL    as   lean_amount , 
NULL    as   life_insurance , 
loan_proposal.ProductCategory    as   loan_category , 
loan_disbursement.loan_cycle    as   loan_cycle , 
loan_proposal.PurposeID    as   loan_purpose , 
loan_sanction.LoanSanctionId    as   loan_sanction_id , 
NULL    as   loan_schedule , 
loan_proposal.LoanStatus    as   loan_status , 
NULL    as   loan_sub_purpose , 
loan_disbursement.UpdatedDate    as   loan_updated_date , 
NULL    as   loan_version , 
customer_info.Langitude    as   longitude , 
NULL    as   lpg_usage , 
customer_other_details.MaritalStatus    as   marital_status , 
customer_other_details.CustomerInfoId    as   marital_status_id , 
 'customer_level'    as   marital_status_id_type , 
customer_other_details.CreatedDate    as   marital_status_report_date , 
NULL    as   marital_status_version , 
customer_other_details.UpdatedDate    as   maritial_status_updated_date , 
customer_other_details.MemberLiteracy    as   member_literacy , 
customer_other_details.Migration    as   migration , 
customer_other_details.NoOfAnimal    as   no_of_animals , 
coalesce(customer_other_details.NoOfDaughters,0) + coalesce(customer_other_details.NoOfSons,0)    as   no_of_childrens , 
coalesce(customer_other_details.DAgeabove15,0) + coalesce(customer_other_details.SAgeabove15,0)    as   no_of_childrens_above_15 , 
coalesce(customer_other_details.DAgebelow15,0) + coalesce(customer_other_details.DAgebelow15,0)    as   no_of_childrens_below_15 , 
customer_other_details.NoOfMembers    as   no_of_dependents , 
customer_other_details.NoOfMembers    as   no_of_members , 
NULL    as   no_of_members_above_60_yrs , 
customer_other_details.NoOfOtherMem    as   no_of_other_memebers , 
coalesce(customer_other_details.DAgebelow15,0) + coalesce(customer_other_details.SAgebelow15,0)    as   no_of_school_going_children , 
NULL    as   no_of_unmarried_children_above_18_yrs , 
customer_other_details.NomineeRelation    as   nominee_relation , 
customer_other_details.CustomerInfoId    as   nominee_relation_id , 
 'customer_level'    as   nominee_relation_id_type , 
customer_other_details.CreatedDate    as   nominee_relation_report_date , 
customer_other_details.UpdatedDate    as   nominee_relation_updated_date , 
NULL    as   nominee_relation_version , 
customer_other_details.NonAgricultureIncome    as   non_agriculture_income , 
loan_proposal.TermPeriod    as   number_of_instalments , 
NULL    as   occupation , 
NULL    as   occupation_id , 
NULL    as   occupation_id_type , 
NULL    as   occupation_report_date , 
NULL    as   occupation_updated_date , 
NULL    as   occupation_version , 
coalesce(customer_cashflow.RDSavings,0) + coalesce(customer_cashflow.SIPSavings,0)    as   other_savings , 
customer_other_details.OwnArea    as   own_area , 
customer_other_details.OwnIrrigate    as   own_irrigation , 
customer_other_details.OwnNonIrrigate    as   own_non_irrigation , 
'Sonata'    as   partner_id , 
transactions.ModeOfPayment    as   payment_mode , 
transactions.PenaltyAmount    as   penality_amount , 
customer_info.CustomerInfoId    as   phone_id , 
 'customer_level'    as   phone_id_type , 
customer_info.mobile_no    as   phone_number , 
customer_info.CreatedDate    as   phone_report_date , 
customer_info.UpdatedDate    as   phone_updated_date , 
NULL    as   phone_version , 
customer_address.ZipCode1    as   pincode , 
customer_address.CustomerInfoId    as   pincode_id , 
'loan_application'    as   pincode_id_type , 
customer_address.CreatedDate    as   pincode_report_date , 
customer_address.UpdatedDate    as   pincode_updated_date , 
NULL    as   pincode_version , 
customer_address.ZipCode1    as   pincode1 , 
customer_address.ZipCode2    as   pincode2 , 
NULL    as   post_office_savings , 
customer_other_details.PovertyStatus    as   poverty_status , 
transactions.PrincipleAmt    as   principal_amount , 
transactions.PrincipleCollected as principal_collected,
loan_disbursement.ProductID    as   product_code , 
loan_disbursement.ProductID    as   product_id , 
NULL    as   product_name , 
loan_disbursement.LoanType    as   product_type , 
loan_sanction.RecoveryAmt    as   recovery_amount , 
NULL    as   recovery_mode , 
region.region_name    as   region , 
coalesce(transactions.PrincipleCollected,0)+coalesce(transactions.InterestCollected,0)    as   repayment_amount , 
transactions.CollectedDate    as   repayment_date , 
loan_proposal.PaymentFrequency    as   repayment_frequency , 
NULL    as   report_date , 
customer_other_details.Response    as   response , 
loan_sanction.SanctionAmt    as   sanctioned_amount , 
loan_sanction.SanctionDate    as   sanctioned_date , 
NULL    as   savings_amount , 
transactions.PrincipleAmt+transactions.InterestAmt    as   schedule_amount , 
transactions.ScheduleDate    as   schedule_date , 
loan_proposal.NewEmpId    as   sfo_id , 
loan_disbursement.DisbursementID    as   sfo_id_id , 
'loan_account_number'    as   sfo_id_id_type , 
loan_disbursement.DisbursementDate    as   sfo_id_report_date , 
loan_disbursement.DisbursementDate    as   sfo_id_updated_date , 
NULL    as   sfo_id_version , 
customer_other_details.ShareArea    as   share_area , 
customer_other_details.ShareIrrigate    as   share_irrigation , 
customer_other_details.ShareNonIrrigate    as   share_non_irrigation , 
NULL    as   smart_phone , 
NULL    as   source_of_data , 
customer_other_details.HusbDOB    as   spouse_dob , 
customer_other_details.HusbandLiteracy    as   spouse_literacy , 
'{folder}' stage_folder_name,
customer_address.state1    as   state1 , 
customer_address.state2    as   state2 , 
NULL    as   sub_occupation , 
NULL    as   sub_village , 
NULL    as   subdivision , 
loan_sanction.TermPeriod    as   term_period , 
customer_other_details.TotalArea    as   total_area , 
transactions.transaction_row_no    as   transaction_row_no , 
transactions.ModeOfPayment    as   transaction_type , 
coalesce(transactions.CollectedDate , transactions.PartialCollectedDate)    as   transactions_date , 
case when customer_cashflow.TVHouseholdAssets  > 0  then True else False end    as   tv , 
loan_disbursement.UpdatedDate    as   updated_at , 
case when customer_cashflow.VehicleAssets  > 0  then True else False end    as   vehicle , 
customer_address.VillageName    as   village , 
customer_address.VillageName    as   village_name , 
NULL    as   washing_machine , 
loan_disbursement.WriteOffAmt    as   writtern_off_amount , 
NULL    as   years_of_stay_in_address , 
NULL    as   zone,
NVL(to_date(transactions.CollectedDate,'yyyy-MM-dd') , to_date('2030-01-01','yyyy-MM-dd')) future_date_postgress

from Customer_info

full outer join customer_cashflow
on Customer_info.CustomerInfoId = customer_cashflow.CustomerInfoId

full outer join customer_other_details
on coalesce(Customer_info.CustomerInfoId,customer_cashflow.CustomerInfoId) = customer_other_details.CustomerInfoId

full outer join customer_address
on coalesce(Customer_info.CustomerInfoId,customer_cashflow.CustomerInfoId,customer_other_details.CustomerInfoId) = customer_address.CustomerInfoId

left join customer_account_details
on 
coalesce(Customer_info.CustomerInfoID,
customer_cashflow.CustomerInfoID,
customer_other_details.CustomerInfoID,
customer_address.CustomerInfoID) = customer_account_details.CustInfoID


full outer join loan_proposal
on customer_account_details.CustInfoID = loan_proposal.CustomerInfoId
and customer_account_details.AccountId = loan_proposal.AccountId

full outer join loan_sanction
on customer_account_details.CustInfoID = loan_sanction.CustomerInfoId
and customer_account_details.AccountId = loan_sanction.AccountId

full outer join loan_disbursement
on customer_account_details.CustInfoID = loan_disbursement.CustomerInfoId
and customer_account_details.AccountId = loan_disbursement.AccountId

left join branch
on trim(loan_disbursement.BranchID) = trim(branch.branch_id)

left join district
on trim(Customer_info.districtID) = trim(district.district_id)

left join division
on trim(customer_info.DivisionId) = trim(division.division_id)

left join hub
on trim(Customer_info.hubID) = trim(hub.hub_id)

left join region
on trim(loan_disbursement.BranchID) = trim(region.branch_id)

FULL OUTER JOIN transactions
on coalesce(loan_disbursement.CustomerInfoID,
		 Customer_info.CustomerInfoID,
		 customer_cashflow.CustomerInfoID,
		 customer_other_details.CustomerInfoID,
		 customer_address.CustomerInfoID,
		 customer_account_details.CustInfoID,
		 loan_sanction.CustomerInfoID,
		 loan_proposal.CustomerInfoID) = transactions.CustomerInfoID
and loan_disbursement.DisbursementID = transactions.DisbursementID
"""

