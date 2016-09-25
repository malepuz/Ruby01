class Pipe_data_reader
  require 'csv'
  @@num_hash=Hash.new()
  @@str_hash=Hash.new()
  @@spc_hash=Hash.new()
  @@src_hash=Hash.new()
  def find_file_in_folder(folder_name, file_type)
    Dir.glob("#{folder_name}/**/#{file_type}")
  end
  def get_num_hash; @@num_hash;  end;
  def get_str_hash; @@str_hash;  end;
  def get_spc_hash; @@spc_hash;  end;
  def get_src_hash; @@src_hash;  end;
  def is_integer?(str)
    !!Integer(str)
  rescue ArgumentError, TypeError
    false
  end
  def read_csv_file(csv_file)
    puts csv_file
    file_array = CSV.read(csv_file)
    column_numbers=file_array[0].all? {|i| is_integer?(i)}
    if column_numbers
      @@num_hash[csv_file]= file_array
    else
      @@str_hash[csv_file]= file_array
    end
  end
  def main(folder_name)
    specs_found = find_file_in_folder(folder_name, 'Spec.csv')
    specs_found.each do |f| read_csv_file(f) end
    @@spc_hash= @@spc_hash.merge(@@str_hash)
    @@str_hash=Hash.new()
    search_found = find_file_in_folder(folder_name, 'SearchFile.csv')
    search_found.each do |f| read_csv_file(f) end
    @@src_hash= @@src_hash.merge(@@str_hash)
    #@@str_hash=Hash.new()
    #files_found = find_file_in_folder(folder_name, '*.csv')
    #files_found[1..250].each do |f| read_csv_file(f) end
  end
end
class String
  #TODO: these functions need improvement
  def to_sql
   self.strip_or_self.gsub("'", '`')
  end
  def strip_or_self
    s= self.strip
    s || self
  end
end
class << nil
  def to_sql
    ''
  end
end
class << Integer
  def to_sql
    self.to_s
  end
end
class << Float
  def to_sql
    self.to_s
  end
end

class Pipe_data_writer
  require 'rubygems'
  require 'sqlite3'
  def sql_exec(db, sql_stm)
    db.execute(sql_stm)
    rescue
      puts "Failed in: #{sql_stm}"
  end
  def main(spc_hash, src_hash)
    db=SQLite3::Database.new('C:/Users/malep/sl3dbs/MorphSu01.sqlite')
    comp_list_group='',pd_file_name='',sql_insert=''
    db.execute('DELETE FROM comp_lists')
    spc_hash.each do |key, value|
      pd_file_name=key.to_sql
      value.each do |cli|
        clq =cli.to_s.gsub('["','').gsub('"]','')
        if clq.start_with?('  ')
          comp_list_item = clq.to_sql
          sql_insert ="INSERT INTO comp_lists (comp_list_group,comp_list_item,pd_file_name) VALUES ('#{comp_list_group}','#{comp_list_item}','#{pd_file_name}');"
          sql_exec(db,sql_insert)
        else
          comp_list_group = clq.to_sql
        end
     end
    end
    db.execute('DELETE FROM comp_files')
    sql_prefix = 'INSERT into comp_files (file_prefix,list_group,comp_cat,to_be_determined,comp_end_cond,standard_organization,pd_file_name) VALUES '
    src_hash.each do |key, value|
      pd_file_name=key.to_sql
      value.each do |cf|
        sql_insert = "#{sql_prefix}('#{cf[0].to_sql}','#{cf[1].to_sql}','#{cf[2].to_sql}','#{cf[3].to_sql}','#{cf[4].to_sql}','#{cf[5].to_sql}','#{pd_file_name}');"
        sql_exec(db,sql_insert)
      end
    end
  end

end

prd = Pipe_data_reader.new
prd.main('C:/Program Files (x86)/Pipedata-Pro11')
 prw = Pipe_data_writer.new
 prw.main(prd.get_spc_hash, prd.get_src_hash)
puts [prd.get_spc_hash,prd.get_src_hash,prd.get_num_hash,prd.get_str_hash]
puts 'Completed, press any key to exit process'
gets
puts "Total Files #{prd.get_spc_hash.size+prd.get_src_hash.size+prd.get_num_hash.size+ prd.get_str_hash.size}"
