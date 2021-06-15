from flask import *  
import csv
import io
import pandas as pd
import mysql.connector as db
import base64

app = Flask(__name__)  
 
@app.route('/')  
def upload():  
    return render_template("index.html")  
 
@app.route('/success', methods = ['POST'])  
def success():  
    if request.method == 'POST': 
        #READING CSV FILE... 
        f = request.files['file'] 
        stream = io.StringIO(f.stream.read().decode("UTF8"), newline=None)
        reader = csv.reader(stream)
        f1=f.filename 
        f.save(f1)  

        '''for x in reader:
            print(x)'''

        #FORMING DATAFRAME...    
        data=[]
        for row in reader:
            data.append(row)
        data=pd.DataFrame(data)
        #print(data)

        #CONNECTING TO MYSQL DATABASE SERVER...
        connection=db.connect(host='localhost',
                        database='grootan',
                        user='root',
                        password='246800')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
        else:
            print("problem")

        #EXTRACTING COLUMN NAMES FOR DATABASE...
        names=[]
        for x in range(len(data.columns)):
            names.append(data[x][0])
        #print(names)

        idx = pd.Index(["%s"%name for name in names])
        #print(idx)
        
        #ENCODING PASSWORDS IF PASSWORD DATAS ARE PRESENT IN THE FILE..
        if "password" in names:
            pos=idx.get_loc('password')
            for x in range(1,len(data.index)):
                pwd=base64.b64encode(data[pos][x].encode("utf-8"))
                data.at[pos,x]=pwd

        #DYNAMICALLY CREATING TABLE...
        sql = """CREATE TABLE %s"""%f1[:-4] +"(\n" +",\n".join([("%s varchar(255)"%name.replace(" ","")) for name in names])+ "\n);"
        print(sql)
        cursor.execute(sql)
        
        #INSERTING ELEMENTS TO TABLE...
        ele=[]
        for x in range(1,len(data.index)):
            ele.clear()
            for y in range(0,len(data.columns)):
                ele.append(data[y][x])
            sql_insert="INSERT INTO %s"%f1[:-4]+" ("+",".join([("%s"%name.replace(" ","")) for name in names])+")"+ " VALUES ("+",".join([("'%s'"%elem) for elem in ele])+");"
            print(sql_insert)
            cursor.execute(sql_insert)
            connection.commit()

        #NUMBER OF RECORDS...
        cnt=data.shape[0]


        return render_template("success.html", name = f1[:-4], count = cnt)  
    return redirect(url_for('upload'))
if __name__ == '__main__':  
    app.run(debug = True)  