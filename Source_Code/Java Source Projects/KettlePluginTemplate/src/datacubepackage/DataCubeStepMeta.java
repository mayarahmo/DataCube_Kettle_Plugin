package datacubepackage;

/*
 * Feito por: GABRIEL GONÇALVES DE CASTRO MARQUES
 * ALUNO DO CURSO DE CIÊNCIA DA COMPUTAÇÃO DA UFRJ
 * 05/05/15
 * DATACUBE TRANSFORMATOR PARA KETTLE(SPOON)
 */

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import br.ufrj.ppgi.greco.kettle.plugin.tools.datatable.DataTable;
import br.ufrj.ppgi.greco.kettle.plugin.tools.datatable.DataTableConverter;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class DataCubeStepMeta extends BaseStepMeta implements
        StepMetaInterface
{

    // Fields for serialization
    public enum Field
    {
        DATA_ROOT_NODE, 
        VERSION,
      
		//Table Dimensions
		MAP_TABLE,
		MAP_TABLE1,
		MAP_TABLE2,
		MAP_TABLE3,
		
		//TABLE 1
        MAP_TABLE_DIMENSIONS_FIELD_NAME,        
        MAP_TABLE_LABELS_FIELD_NAME,
        MAP_TABLE_URI_FIELD_NAME,
        MAP_TABLE_URI_TYPE_FIELD_NAME,
        
        //TABLE 2
        MAP_TABLE_MEASURE_FIELD_NAME,
        MAP_TABLE_MEASURE_LABEL_FIELD_NAME,
        MAP_TABLE_MEASURE_URI_FIELD_NAME,
        MAP_TABLE_MEASURE_URI_TYPE_FIELD_NAME,
        
        //TABLE 3
        PREFIXES,
		
		//TABLE 4
        OUTPUT_UNITY_FIELD_NAME,
        
        //TABLE 5
        MAP_TABLE_HIERARCHY_FIELD_NAME,
        MAP_TABLE_HIERARCHY_DE_FIELD_NAME,
        MAP_TABLE_HIERARCHY_LABEL_FIELD_NAME,
        MAP_TABLE_HIERARCHY_PARA_FIELD_NAME,

    }

    // Campos do step
    
    //Table 1
    private DataTable<String> mapTable;  
    //Table 2
    private DataTable<String> mapTable1; 
    //Table 3
    List<List<String>> prefixes;
	//Table 4
    private String unity;
    //Table 5
    private DataTable<String> mapTable2;
	private String vocab4;
	private String vocab3;
	private String vocab2;
	private String vocab1;
	private String vocab5;
	private String vocab6;
	private String vocab7;
	private String vocab8;
	private String vocab9;
	private String vocab10;
	private String vocab11;
	private String vocab12;

    public DataCubeStepMeta()
    {
        setDefault();
    }

    // TODO Validar todos os campos para dar feedback ao usuario!
    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
            StepMeta stepMeta, RowMetaInterface prev, String[] input,
            String[] output, RowMetaInterface info)
    {
        CheckResultInterface ok = new CheckResult(CheckResult.TYPE_RESULT_OK,
                "", stepMeta);
        remarks.add(ok);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta,
            StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans)
    {
        return new DataCubeStep(stepMeta, stepDataInterface,
                copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData()
    {
        return new DataCubeStepData();
    }

    @Override
    public String getDialogClassName()
    {
        return DataCubeStepDialog.class.getName();
    }

    // Carregar campos a partir do XML de um .ktr
    @SuppressWarnings("unchecked")
    @Override
    public void loadXML(Node stepDomNode, List<DatabaseMeta> databases,
            Map<String, Counter> sequenceCounters) throws KettleXMLException
    {	
        try
        {

        	
            XStream xs = new XStream(new DomDriver());
            xs.alias("DataTable", DataTable.class);
            xs.registerConverter(new DataTableConverter());

            StringWriter sw = new StringWriter();
            Transformer t = TransformerFactory.newInstance().newTransformer();
            // IPC: se algum colocar um caracter, seja qual for, no getXML() o
            // getFirstChild() para de funcionar aqui!
            t.transform(
                    new DOMSource(XMLHandler.getSubNode(stepDomNode,
                            Field.DATA_ROOT_NODE.name()).getFirstChild()),
                    new StreamResult(sw));

            Map<String, Object> data = (Map<String, Object>) xs.fromXML(sw
                    .toString());

            int version = (int) (Integer) data.get(Field.VERSION.name());

            switch (version)
            {
                case 1:
                    mapTable = (DataTable<String>) data.get(Field.MAP_TABLE
                            .name());
                    mapTable1 = (DataTable<String>) data.get(Field.MAP_TABLE1
                            .name());
                    mapTable2 = (DataTable<String>) data.get(Field.MAP_TABLE2
                            .name());
                	
                    
                    prefixes = (List<List<String>>) data.get(Field.PREFIXES.name());
                    
                    
                    unity = (String) data.get(Field.OUTPUT_UNITY_FIELD_NAME.name());

                    break;
                default:
                    setDefault();
            }
        }
        catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    // Gerar XML para salvar um .ktr
    @Override
    public String getXML() throws KettleException
    {


        XStream xs = new XStream();
        xs.alias("DataTable", DataTable.class);
        xs.registerConverter(new DataTableConverter());

        Map<String, Object> data = new HashMap<String, Object>();
        data.put(Field.VERSION.name(), 1);
        data.put(Field.MAP_TABLE.name(), mapTable);
        data.put(Field.MAP_TABLE1.name(), mapTable1);
        data.put(Field.MAP_TABLE2.name(), mapTable2);
        
        data.put(Field.PREFIXES.name(), prefixes);
        
        data.put(Field.OUTPUT_UNITY_FIELD_NAME.name(), unity);

        return String.format("<%1$s>%2$s</%1$s>", Field.DATA_ROOT_NODE.name(),
                xs.toXML(data));
    }

    //Carregar campos a partir do repositorio
    @Override
    public void readRep(Repository repository, ObjectId stepIdInRepository,
            List<DatabaseMeta> databases, Map<String, Counter> sequenceCounters)
            throws KettleException
    {
    	

    	
    	
        try
        {
            int version = (int) repository.getStepAttributeInteger(
                    stepIdInRepository, Field.VERSION.name());

            switch (version)
            {
                case 1:
                    int nrLines = (int) repository.getStepAttributeInteger(
                            stepIdInRepository, "nr_lines");
                    
                    
                    //TABLE 1
                    mapTable = new DataTable<String>(Field.MAP_TABLE.name(),
                            Field.MAP_TABLE_DIMENSIONS_FIELD_NAME.name(),
                            Field.MAP_TABLE_LABELS_FIELD_NAME.name(),
                            Field.MAP_TABLE_URI_FIELD_NAME.name(),
                            Field.MAP_TABLE_URI_TYPE_FIELD_NAME.name());
                    String[] fields = mapTable.getHeader().toArray(
                            new String[0]);
                    for (int i = 0; i < nrLines; i++)
                    {
                        int nrfields = fields.length;
                        String[] line = new String[nrfields];

                        for (int f = 0; f < nrfields; f++)
                        {
                            line[f] = repository.getStepAttributeString(
                                    stepIdInRepository, i, fields[f]);
                        }
                        mapTable.add(line);
                    }
                    
                    
                    //TABLE 2
                    mapTable = new DataTable<String>(Field.MAP_TABLE1.name(),
                            Field.MAP_TABLE_MEASURE_FIELD_NAME.name(),
                            Field.MAP_TABLE_MEASURE_LABEL_FIELD_NAME.name(),
                            Field.MAP_TABLE_MEASURE_URI_FIELD_NAME.name(),
                            Field.MAP_TABLE_MEASURE_URI_TYPE_FIELD_NAME.name());
                    fields = mapTable1.getHeader().toArray(
                            new String[0]);
                    for (int i = 0; i < nrLines; i++)
                    {
                        int nrfields = fields.length;
                        String[] line = new String[nrfields];

                        for (int f = 0; f < nrfields; f++)
                        {
                            line[f] = repository.getStepAttributeString(
                                    stepIdInRepository, i, fields[f]);
                        }
                        mapTable1.add(line);
                    }
                
                    
                    
                    //Table 4
                    unity = repository.getStepAttributeString(stepIdInRepository, Field.OUTPUT_UNITY_FIELD_NAME.name());
                    
                    
                    //Table 5
                    mapTable = new DataTable<String>(Field.MAP_TABLE2.name(),
                            Field.MAP_TABLE_HIERARCHY_FIELD_NAME.name(),
                            Field.MAP_TABLE_HIERARCHY_DE_FIELD_NAME.name(),
                            Field.MAP_TABLE_HIERARCHY_LABEL_FIELD_NAME.name(),
                            Field.MAP_TABLE_HIERARCHY_PARA_FIELD_NAME.name());
                    fields = mapTable2.getHeader().toArray(
                            new String[0]);
                    for (int i = 0; i < nrLines; i++)
                    {
                        int nrfields = fields.length;
                        String[] line = new String[nrfields];

                        for (int f = 0; f < nrfields; f++)
                        {
                            line[f] = repository.getStepAttributeString(
                                    stepIdInRepository, i, fields[f]);
                        }
                        mapTable2.add(line);
                    }
                 
                    
                    
                    break;
                default:
                    setDefault();
            }
        }
        catch (Exception e)
        {
            throw new KettleException(
                    "Unable to read step information from the repository for id_step="
                            + stepIdInRepository, e);
        }

    }

    @Override
    public void saveRep(Repository repository, ObjectId idOfTransformation,
            ObjectId idOfStep) throws KettleException
    {
    	

    	
        try
        {
            repository.saveStepAttribute(idOfTransformation, idOfStep,
                    Field.VERSION.name(), 1);

            //TABLE 1
            int linhas = (int) mapTable.size();
            int colunas = mapTable.getHeader().size();
            repository.saveStepAttribute(idOfTransformation, idOfStep,
                    "nr_lines", linhas);
            for (int i = 0; i < linhas; i++)
            {
                for (int f = 0; f < colunas; f++)
                {
                    repository.saveStepAttribute(idOfTransformation, idOfStep,
                            i, mapTable.getHeader().get(f),
                            mapTable.getValue(i, f));
                }
            }
            
            
            //TABLE 2
            linhas = (int) mapTable1.size();
            colunas = mapTable1.getHeader().size();
            repository.saveStepAttribute(idOfTransformation, idOfStep,
                    "nr_lines", linhas);
            for (int i = 0; i < linhas; i++)
            {
                for (int f = 0; f < colunas; f++)
                {
                    repository.saveStepAttribute(idOfTransformation, idOfStep,
                            i, mapTable1.getHeader().get(f),
                            mapTable1.getValue(i, f));
                }
            }
            
            
            //Table 3
            repository.saveStepAttribute(idOfTransformation, idOfStep,
                    Field.PREFIXES.name(), new XStream().toXML(prefixes));
            
            //Table 4
            repository.saveStepAttribute(idOfTransformation, idOfStep,Field.OUTPUT_UNITY_FIELD_NAME.name(), unity);

            
            //TABLE 5
            linhas = (int) mapTable2.size();
            colunas = mapTable2.getHeader().size();
            repository.saveStepAttribute(idOfTransformation, idOfStep,
                    "nr_lines", linhas);
            for (int i = 0; i < linhas; i++)
            {
                for (int f = 0; f < colunas; f++)
                {
                    repository.saveStepAttribute(idOfTransformation, idOfStep,
                            i, mapTable2.getHeader().get(f),
                            mapTable2.getValue(i, f));
                }
            }
            
           
        }
        catch (Exception e)
        {
            throw new KettleException(
                    "Unable to save step information to the repository for id_step= "+ idOfStep, e);
        }
    }

    // Inicializacoes default
    @Override
    public void setDefault()
    {

    	//TABLE 1
        mapTable = new DataTable<String>(Field.MAP_TABLE.name(),
                Field.MAP_TABLE_DIMENSIONS_FIELD_NAME.name(),
                Field.MAP_TABLE_LABELS_FIELD_NAME.name(),
                Field.MAP_TABLE_URI_FIELD_NAME.name(),
                Field.MAP_TABLE_URI_TYPE_FIELD_NAME.name());
        
        //TABLE 2
        mapTable1 = new DataTable<String>(Field.MAP_TABLE1.name(),
		        Field.MAP_TABLE_MEASURE_FIELD_NAME.name(),
		        Field.MAP_TABLE_MEASURE_LABEL_FIELD_NAME.name(),
		        Field.MAP_TABLE_MEASURE_URI_FIELD_NAME.name(),
		        Field.MAP_TABLE_MEASURE_URI_TYPE_FIELD_NAME.name());
      
        //TABLE 3
        prefixes = new ArrayList<List<String>>();
        
		//TABLE 4
        unity = "Descrição da unidade tratada no DataSet";
        
     	//TABLE 5
        mapTable2 = new DataTable<String>(Field.MAP_TABLE2.name(),
		        Field.MAP_TABLE_HIERARCHY_FIELD_NAME.name(),
		        Field.MAP_TABLE_HIERARCHY_DE_FIELD_NAME.name(),
		        Field.MAP_TABLE_HIERARCHY_LABEL_FIELD_NAME.name(),
		        Field.MAP_TABLE_HIERARCHY_PARA_FIELD_NAME.name());
        
    }

    /**
     * It describes what each output row is going to look like
     */
    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name,
            RowMetaInterface[] info, StepMeta nextStep, VariableSpace space)
            throws KettleStepException
    {
        ValueMeta field = null;

        field = new ValueMeta("",
                ValueMetaInterface.TYPE_STRING);
        field.setOrigin("");
        inputRowMeta.addValueMeta(field);

        field = new ValueMeta("",
                ValueMetaInterface.TYPE_STRING);
        field.setOrigin("");
        inputRowMeta.addValueMeta(field);

        field = new ValueMeta("",
                ValueMetaInterface.TYPE_STRING);
        field.setOrigin("");
        inputRowMeta.addValueMeta(field);

    }

    
    // Getters & Setters
    public List<List<String>> getPrefixes()
    {
        return prefixes;
    }

    public void setPrefixes(List<List<String>> prefixes)
    {
        this.prefixes = prefixes;
    }
    
    public DataTable<String> getMapTable()
    {
        return mapTable;
    }
    
    public DataTable<String> getMapTable1()
    {
        return mapTable1;
    }
    
    public DataTable<String> getMapTable2()
    {
        return mapTable2;
    }
    
    public String getunity()
    {
        return unity;
    }

    public void setunity(String unity)
    {
        this.unity = unity;
    }
       
    public String getvocab1()
    {
        return vocab1;
    }

    public void setvocab1(String vocab1)
    {
        this.vocab1 = vocab1;
    }
    
    public String getvocab2()
    {
        return vocab2;
    }

    public void setvocab2(String vocab2)
    {
        this.vocab2 = vocab2;
    }
    public String getvocab3()
    {
        return vocab3;
    }

    public void setvocab3(String vocab3)
    {
        this.vocab3 = vocab3;
    }
    public String getvocab4()
    {
        return vocab4;
    }

    public void setvocab4(String vocab4)
    {
        this.vocab4 = vocab4;
    }
    public String getvocab5()
    {
        return vocab5;
    }

    public void setvocab5(String vocab5)
    {
        this.vocab5 = vocab5;
    }
    
    public String getvocab6()
    {
        return vocab6;
    }

    public void setvocab6(String vocab6)
    {
        this.vocab6 = vocab6;
    }
    public String getvocab7()
    {
        return vocab7;
    }

    public void setvocab7(String vocab7)
    {
        this.vocab7 = vocab7;
    }
    public String getvocab8()
    {
        return vocab8;
    }

    public void setvocab8(String vocab8)
    {
        this.vocab8 = vocab8;
    }
    public String getvocab9()
    {
        return vocab9;
    }

    public void setvocab9(String vocab9)
    {
        this.vocab9 = vocab9;
    }
    public String getvocab10()
    {
        return vocab10;
    }

    public void setvocab10(String vocab10)
    {
        this.vocab10 = vocab10;
    }
    public String getvocab11()
    {
        return vocab11;
    }

    public void setvocab11(String vocab11)
    {
        this.vocab11 = vocab11;
    }
    
    public String getvocab12()
    {
        return vocab12;
    }

    public void setvocab12(String vocab12)
    {
        this.vocab12 = vocab2;
    }
}